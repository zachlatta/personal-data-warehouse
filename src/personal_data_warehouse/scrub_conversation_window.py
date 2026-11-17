"""Host-side token scrub controller. Run on the production Docker host as root.

Only plan/verify read production data. Apply stops the app and entire Dagster
container, including its daemon, run subprocesses and resident landing workers.
The original schedule/sensor states are never modified. No credentials in argv
or stdout; the runtime environment stays in a private local file.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile

RUNTIME_NAME='pdw-conversation-token-scrub'


def docker(args, *, capture=True):
    result=subprocess.run(['docker',*args],check=True,text=True,
                          stdout=subprocess.PIPE if capture else None)
    return result.stdout if capture else ''


def save(path, value, *, replace=False):
    if replace:
        fd,name=tempfile.mkstemp(dir=path.parent,prefix=path.name+'.')
        with os.fdopen(fd,'w') as stream: json.dump(value,stream)
        os.replace(name,path)
        return
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as stream: json.dump(value,stream)


def snapshot(state):
    code="""import json,os
from dagster import DagsterInstance, RunsFilter, DagsterRunStatus
key='DAGSTER_POSTGRES_URL'
os.environ[key]=os.environ[key].replace('postgres://','postgresql://',1)
with DagsterInstance.get() as instance:
 print(json.dumps({'schedules':sorted([(s.name,s.instigator_type.value,s.status.value) for s in instance.all_instigator_state()]),
 'runs':[(r.run_id,r.job_name,r.status.value) for r in instance.get_runs(filters=RunsFilter(statuses=[DagsterRunStatus.QUEUED,DagsterRunStatus.STARTING,DagsterRunStatus.STARTED,DagsterRunStatus.CANCELING]))]}))
"""
    return json.loads(docker(['exec',state['writers'][0]['Id'],'/app/.venv/bin/python','-c',code]))


def assert_current(state):
    # Refuse a deployment replacement or an independent run launcher. The
    # checked-in Dagster launcher uses subprocesses INSIDE the stopped container.
    writers=state['writers']
    for label,item in zip(('personal-data-warehouse-dagster','app'),writers,strict=True):
        ids=docker(['ps','-q','--no-trunc','--filter',f'label=coolify.resourceName={label}']).split()
        if ids != [item['Id']]:
            raise RuntimeError('writer inventory changed; suspend deployments and replan')
        live=json.loads(docker(['inspect',item['Id']]))[0]
        if not live['State']['Running'] or live['Image'] != item['Image']:
            raise RuntimeError('writer container changed; take a fresh maintenance plan')
    external=docker(['ps','-q','--filter','label=dagster/run_id']).strip()
    if external: raise RuntimeError('independent Dagster run containers remain; drain them before maintenance')


def runtime(directory, state, module, *args):
    mounts=['--volumes-from',state['writers'][0]['Id']+':ro'] if state.get('writers') else []
    docker(['run','--rm','--name',RUNTIME_NAME,*mounts,'--network',state['network'],'--env-file',str(directory/'runtime.env'),
            '--volume',f'{directory}:/scrub','--entrypoint','/app/.venv/bin/python',state['image'],
            '-m',f'personal_data_warehouse.scrub_conversation_{module}',
            '--scope','tokens','--plan',f'/scrub/token-{module}-plan.json',*args],capture=False)


def plan(directory):
    directory.mkdir(mode=0o700,parents=True,exist_ok=False)
    writers=[]
    for label in ('personal-data-warehouse-dagster','app'):
        ids=docker(['ps','-q','--filter',f'label=coolify.resourceName={label}']).split()
        if len(ids)!=1: raise RuntimeError('expected exactly one running writer container per resource')
        item=json.loads(docker(['inspect',ids[0]]))[0]
        writers.append(item)
    dagster=writers[0]
    env=dagster['Config']['Env']
    if any('\n' in v or '\r' in v for v in env):
        raise RuntimeError('multiline environment requires a reviewed runtime configuration')
    fd=os.open(directory/'runtime.env',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as stream: stream.write('\n'.join(env)+'\n')
    state=dict(writers=[{**{k:w[k] for k in ('Id','Image','Name')},'State':{'Running':w['State']['Running']}} for w in writers],
               image=dagster['Image'],network=dagster['HostConfig']['NetworkMode'])
    save(directory/'state.json',state)
    assert_current(state)
    save(directory/'before-plan.json',snapshot(state))
    runtime(directory,state,'objects')
    runtime(directory,state,'cards')


def restore(directory):
    state=json.loads((directory/'state.json').read_text())
    running=docker(['ps','-q','--filter',f'name=^/{RUNTIME_NAME}$']).strip()
    if running:
        # A controller interruption must roll back the maintenance connection
        # before any live writer is restored. Never start writers on stop failure.
        docker(['stop','--time','30',running])
    failures=[]
    for writer in reversed(state['writers']):
        if not writer.get('State',{'Running':True})['Running']: continue
        try: docker(['start',writer['Id']])
        except Exception: failures.append(writer['Id'])
    if failures: raise RuntimeError('writer restoration failed; rerun restore using the saved state')


def apply(directory):
    state=json.loads((directory/'state.json').read_text())
    assert_current(state)
    save(directory/'before-stop.json',snapshot(state),replace=True)
    # Register restoration BEFORE the first stop: docker can partially stop a
    # list before returning an error. SIGKILL/host loss needs explicit restore.
    try:
        docker(['stop','--time','30',*[w['Id'] for w in state['writers']]])
        runtime(directory,state,'objects','--apply')
        runtime(directory,state,'cards','--apply','--session-batches','--planned-only',
                '--writers-paused','--max-canonical-rows','10000','--window-seconds','60')
    finally:
        restore(directory)


def verify(directory):
    state=json.loads((directory/'state.json').read_text())
    assert_current(state)
    after=snapshot(state)
    before=json.loads((directory/'before-stop.json').read_text())
    if after['schedules'] != before['schedules']:
        raise RuntimeError('schedule/sensor status changed; inspect saved state before proceeding')
    runtime(directory,state,'objects','--verify')
    runtime(directory,state,'cards','--verify')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase',choices=('plan','apply','restore','verify'))
    parser.add_argument('--state-dir',type=Path,required=True)
    args=parser.parse_args()
    directory=args.state_dir.expanduser().resolve()
    def interrupted(*_): raise InterruptedError('maintenance interrupted; restoration required')
    signal.signal(signal.SIGTERM,interrupted)
    globals()[args.phase](directory)


if __name__=='__main__': main()
