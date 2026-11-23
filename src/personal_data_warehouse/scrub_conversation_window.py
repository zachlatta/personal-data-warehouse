"""Host-side credential scrub controller. Run on the production Docker host as root.

Only plan/verify read production data; the archive-* phases repair the Drive
archive live and never stop a writer. Apply stops the app and entire Dagster
container, including its daemon, run subprocesses and resident landing workers.
The original schedule/sensor states are never modified. No credentials in argv
or stdout; the runtime environment stays in a private local file until a
clean verify removes it.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile

RUNTIME_NAME='pdw-conversation-credential-scrub'
MAINTENANCE_ROOT=Path('/var/lib/pdw-maintenance')


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
    # No writer volumes: the scrub reaches Postgres and Drive through the
    # environment only, and the Dagster container mounts the Docker socket.
    docker(['run','--rm','--name',RUNTIME_NAME,'--network',state['network'],'--env-file',str(directory/'runtime.env'),
            '--volume',f'{directory}:/scrub','--entrypoint','/app/.venv/bin/python',state['image'],
            '-m',f'personal_data_warehouse.scrub_conversation_{module}',
            '--scope','credentials','--plan',f'/scrub/credential-{module}-plan.json',*args],capture=False)


def plan(directory, archive_args=None):
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
    state=dict(writers=[{k:w[k] for k in ('Id','Image','Name')} for w in writers],
               image=dagster['Image'],network=dagster['HostConfig']['NetworkMode'])
    save(directory/'state.json',state)
    assert_current(state)
    save(directory/'before-plan.json',snapshot(state))
    if archive_args is None: runtime(directory,state,'cards')
    else:
        save(directory/'archive-scope.json',archive_args)
        runtime(directory,state,'objects',*archive_args)


def restore(directory):
    state=json.loads((directory/'state.json').read_text())
    running=docker(['ps','-q','--filter',f'name=^/{RUNTIME_NAME}$']).strip()
    if running:
        # A controller interruption must roll back the maintenance connection
        # before any live writer is restored. Never start writers on stop failure.
        docker(['stop','--time','30',running])
    failures=[]
    # plan records only running writers, so every saved writer is restarted.
    for writer in reversed(state['writers']):
        try: docker(['start',writer['Id']])
        except Exception: failures.append(writer['Id'])
    if failures: raise RuntimeError('writer restoration failed; rerun restore using the saved state')


def apply(directory, max_canonical_rows=10000, max_source_rows=500, max_plan_rows=20000, max_groups=None):
    # A group that overruns its 60-second transaction rolls back; rerunning
    # apply with a cap just above the largest session isolates it.
    if not 0<max_canonical_rows<=30000: raise ValueError('--max-canonical-rows must be 1..30000')
    if max_groups is not None and max_groups<1: raise ValueError('--max-groups must be positive')
    state=json.loads((directory/'state.json').read_text())
    assert_current(state)
    save(directory/'before-stop.json',snapshot(state),replace=True)
    # Register restoration BEFORE the first stop: docker can partially stop a
    # list before returning an error. SIGKILL/host loss needs explicit restore.
    try:
        docker(['stop','--time','30',*[w['Id'] for w in state['writers']]])
        runtime(directory,state,'cards','--apply','--session-batches','--planned-only',
                '--writers-paused','--max-canonical-rows',str(max_canonical_rows),'--window-seconds','60',
                '--max-source-rows',str(max_source_rows),'--max-plan-rows',str(max_plan_rows),
                *(['--max-groups',str(max_groups)] if max_groups else []))
    finally:
        restore(directory)


def verify(directory):
    state=json.loads((directory/'state.json').read_text())
    assert_current(state)
    after=snapshot(state)
    before=json.loads((directory/'before-stop.json').read_text())
    if after['schedules'] != before['schedules']:
        raise RuntimeError('schedule/sensor status changed; inspect saved state before proceeding')
    runtime(directory,state,'cards','--verify')
    # The environment file is a copy of the Dagster secrets; keep it only
    # until the scans pass, so a failed verify can be rerun.
    (directory/'runtime.env').unlink(missing_ok=True)


def cleanup(directory):
    """Remove only controller-owned files from a direct maintenance child.

    Refuse symlinks, unknown entries and active runtimes before deleting anything.
    Incomplete plans (including the old archive-first plan) need no state.json.
    """
    if '..' in directory.parts:
        raise ValueError('cleanup refuses parent references in the state path')
    if directory.is_symlink() or directory.parent.resolve()!=MAINTENANCE_ROOT.resolve():
        raise ValueError('cleanup requires a direct directory under the maintenance root')
    if not directory.exists(): return
    allowed={'runtime.env','state.json','before-plan.json','before-stop.json',
             'credential-cards-plan.json','credential-objects-plan.json','archive-scope.json'}
    entries=list(directory.iterdir())
    # save(replace=True) and the cards plan writer leave '<name>.<random>'
    # temporaries behind if killed between create and rename.
    def owned(name): return name in allowed or any(name.startswith(a+'.') for a in allowed)
    if any(not owned(p.name) or p.is_symlink() or not p.is_file() for p in entries):
        raise ValueError('state contains unowned entries; nothing removed')
    if docker(['ps','-q','--filter',f'name=^/{RUNTIME_NAME}$']).strip():
        raise RuntimeError('maintenance runtime is active; restore before cleanup')
    state_path=directory/'state.json'
    if state_path.exists():
        # Old deployment containers may have been removed. Existing saved
        # writers must be running; never discard the only restoration record.
        for writer in json.loads(state_path.read_text())['writers']:
            ids=docker(['ps','-aq','--no-trunc','--filter',f"id={writer['Id']}"]).split()
            if ids and not json.loads(docker(['inspect',writer['Id']]))[0]['State']['Running']:
                raise RuntimeError('saved writer is stopped; restore before cleanup')
    for path in entries: path.unlink()
    directory.rmdir()


def archive(directory, phase):
    state=json.loads((directory/'state.json').read_text())
    assert_current(state)
    runtime(directory,state,'objects','--'+phase)
    if phase=='verify': (directory/'runtime.env').unlink(missing_ok=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase',choices=('plan','apply','restore','verify','cleanup',
                                        'archive-plan','archive-apply','archive-verify'))
    parser.add_argument('--state-dir',type=Path,required=True)
    parser.add_argument('--max-canonical-rows',type=int,default=10000,help='apply only: canonical rows per session group')
    parser.add_argument('--max-source-rows',type=int,default=500,help='apply only: matching source rows the whole plan may hold')
    parser.add_argument('--max-plan-rows',type=int,default=20000,help='apply only: source and derived rows the whole plan may hold')
    parser.add_argument('--max-groups',type=int,help='apply only: session groups per writer outage; rerun apply to resume')
    parser.add_argument('--exported-from')
    parser.add_argument('--exported-before')
    parser.add_argument('--max-objects',type=int,default=70000)
    parser.add_argument('--max-bytes',type=int,default=5_000_000_000)
    args=parser.parse_args()
    directory=args.state_dir.expanduser().absolute()
    def interrupted(*_): raise InterruptedError('maintenance interrupted; restoration required')
    signal.signal(signal.SIGTERM,interrupted)
    if args.phase=='archive-plan':
        if not args.exported_from or not args.exported_before: parser.error('archive-plan requires both UTC bounds')
        plan(directory,['--exported-from',args.exported_from,'--exported-before',args.exported_before,
                        '--max-objects',str(args.max_objects),'--max-bytes',str(args.max_bytes)])
    elif args.phase.startswith('archive-'): archive(directory,args.phase.removeprefix('archive-'))
    elif args.phase=='apply': apply(directory,args.max_canonical_rows,args.max_source_rows,args.max_plan_rows,args.max_groups)
    else: globals()[args.phase](directory)


if __name__=='__main__': main()
