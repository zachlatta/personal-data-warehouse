"""Explicit scrub of archived/inbox conversation batches (dry-run by default).

Read-only --plan discovery runs while writers remain live. Reuse that ID-only
plan with --apply; stop writers/consumers only for candidate replacement. Resume
workers before the full read-only --verify archive scan. Replacement uploads
contain only sanitized bytes, verified before deleting the original object.
"""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import tempfile

from personal_data_warehouse.card_redaction import is_card, redact
from personal_data_warehouse.card_scrub_selector import ScrubSelector, add_selector_arguments, selector_from_args
from personal_data_warehouse.card_prefilter import ConfirmationPool, available_cpus, might_contain_card


def _line_matches(raw,pool):
    lines=raw.splitlines(keepends=True)
    indexes=[i for i,line in enumerate(lines) if might_contain_card(line)]
    matches={}
    for start in range(0,len(indexes),max(500,pool.workers*64)):
        batch=indexes[start:start+max(500,pool.workers*64)]
        for index,n in zip(batch,pool.counts([{'text':lines[i]} for i in batch]),strict=True):
            if n:matches[index]=n
    return lines,matches


def discover_objects(store, *, workers=1, selector=None):
    """Read-only/live discovery; only object/line identities and counts survive."""
    counts = {'objects': 0, 'lines': 0, 'matches': 0}
    candidates=[]
    with ConfirmationPool(workers, selector) as pool:
        for listing in store.list_objects(kind='agent_sessions_export_batch'):
            raw = gzip.decompress(store.get_object(listing.ref)).decode('utf-8')
            _,matches=_line_matches(raw,pool)
            if not matches:continue
            counts['objects']+=1;counts['lines']+=len(matches);counts['matches']+=sum(matches.values())
            ref={k:v for k,v in listing.ref.items() if k in ('storage_backend','storage_file_id','storage_key')}
            candidates.append({'ref':ref,'lines':list(matches),'matches':sum(matches.values())})
    plan = {'version':1,'candidates':candidates,'counts':counts}
    if selector and selector.scope != "all": plan.update(selector=selector.specification,left_alone=pool.left_alone)
    return plan


def _validate_object_plan(plan):
    if "selector" in plan:
        from personal_data_warehouse.card_scrub_selector import validate_specification
        validate_specification(plan["selector"])
        if type(plan.get("left_alone")) is not int or plan["left_alone"] < 0: raise ValueError("invalid left-alone count")
    if set(plan)-{"selector","left_alone"}!={'version','candidates','counts'} or plan['version']!=1:
        raise ValueError('invalid object discovery plan')
    for candidate in plan['candidates']:
        if set(candidate)!={'ref','lines','matches'} or not set(candidate['ref']) <= {'storage_backend','storage_file_id','storage_key'}:
            raise ValueError('invalid object identity')
        if not candidate['ref'].get('storage_file_id') or any(not isinstance(v,str) or is_card(v) for v in candidate['ref'].values()):
            raise ValueError('sensitive or invalid object identity')
        if any(type(i) is not int or i<0 for i in candidate['lines']):raise ValueError('invalid object line identity')
    if plan['counts'] != dict(objects=len(plan['candidates']),lines=sum(len(c['lines']) for c in plan['candidates']),matches=sum(c['matches'] for c in plan['candidates'])):
        raise ValueError('object counts do not match identities')


def scrub_objects(store, *, apply=False, plan=None, workers=1, selector=None):
    plan=plan if plan is not None else discover_objects(store,workers=workers,selector=selector)
    _validate_object_plan(plan)
    if plan.get("selector",ScrubSelector().specification) != (selector or ScrubSelector()).specification:
        raise ValueError("plan selector differs from current selector")
    if not apply:return plan['counts']
    # Metadata listing is cheap; never download the entire archive again in
    # the maintenance window. Post-resume --verify performs the full read scan.
    listings={item.ref['storage_file_id']:item for item in store.list_objects(kind='agent_sessions_export_batch')}
    for candidate in plan['candidates']:
        listing=listings.get(candidate['ref']['storage_file_id'])
        if listing is None:continue
        raw=gzip.decompress(store.get_object(listing.ref)).decode('utf-8')
        with ConfirmationPool(workers, selector) as pool:lines,matches=_line_matches(raw,pool)
        if not matches:continue
        output=[(selector.redact if selector else redact)(line) if i in matches else line for i,line in enumerate(lines)]
        with ConfirmationPool(workers, selector) as pool:
            if _line_matches(''.join(output),pool)[1]:
                raise RuntimeError('sanitized object still has candidates; no replacement written')
        clean = gzip.compress(''.join(output).encode(), mtime=0)
        digest = hashlib.sha256(clean).hexdigest()
        props = dict(listing.app_properties)
        props['content_sha256'] = props['batch_sha256'] = digest
        # No original bytes ever reach a temporary file.
        with tempfile.TemporaryDirectory(prefix='.scrub-', dir=Path.cwd()) as directory:
            path = Path(directory) / 'sanitized.jsonl.gz'
            path.write_bytes(clean)
            replacement = store.put_file(path=path, object_key=listing.ref['storage_key'], content_sha256=digest, content_type='application/gzip', app_properties=props, kind='agent_sessions_export_batch')
        if hashlib.sha256(store.get_object(replacement)).hexdigest() != digest:
            raise RuntimeError('sanitized object verification failed')
        if replacement['storage_file_id'] == listing.ref['storage_file_id']:
            raise RuntimeError('replacement must have a distinct object id')
        store.delete_object(listing.ref)
    return plan['counts']


def main():
    from personal_data_warehouse.config import load_settings
    from personal_data_warehouse.defs.agent_sessions_drive_ingest import _agent_sessions_object_store
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--verify', action='store_true',help='full read-only verification after workers resume')
    parser.add_argument('--plan',type=Path,help='new ID-only plan for discovery; existing plan for apply')
    parser.add_argument('--workers',type=int,default=available_cpus())
    add_selector_arguments(parser)
    args = parser.parse_args()
    selector = selector_from_args(args)
    if args.workers<1:parser.error('--workers must be positive')
    if args.apply and args.verify:parser.error('--apply and --verify are mutually exclusive')
    if not args.verify and args.plan is None:parser.error('--plan is required')
    store = _agent_sessions_object_store(load_settings(require_gmail=False, require_agent_sessions=True))
    if args.apply:
        plan=json.loads(args.plan.read_text())
        scrub_objects(store,apply=True,plan=plan,workers=args.workers,selector=selector)
        print(json.dumps({'baseline':plan['counts'],'verification_pending':True}))
    elif args.verify:
        print(json.dumps({'after':discover_objects(store,workers=args.workers,selector=selector)['counts']}))
    else:
        plan=discover_objects(store,workers=args.workers,selector=selector)
        _validate_object_plan(plan)
        fd=os.open(args.plan,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
        with os.fdopen(fd,'w') as stream:json.dump(plan,stream)
        print(json.dumps({'before':plan['counts'],'left_alone':plan.get('left_alone')}))


if __name__ == '__main__':
    main()
