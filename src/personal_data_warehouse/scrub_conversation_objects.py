"""Explicit scrub of archived/inbox conversation batches (dry-run by default).

Read-only --plan discovery and candidate replacement run while writers remain
live. Reuse that ID-only plan with --apply and its bounded export interval with
--verify. Replacement uploads
contain only sanitized bytes, verified before deleting the original object.
"""
import argparse
import gzip
import hashlib
import json
import os
import sys
from pathlib import Path
import tempfile
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import threading

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


def validate_scan_scope(scope):
    if set(scope)!={'exported_from','exported_before','max_objects','max_bytes'}:
        raise ValueError('invalid archive scan scope')
    try:
        dates=[datetime.strptime(scope[k],'%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
               for k in ('exported_from','exported_before')]
    except (ValueError,TypeError): raise ValueError('scope dates must be UTC seconds') from None
    if dates[0]>=dates[1] or any(type(scope[k]) is not int or scope[k]<0 for k in ('max_objects','max_bytes')):
        raise ValueError('invalid archive scan bounds')
    return dates


def scoped_listings(store, scope):
    listings=store.list_objects(kind='agent_sessions_export_batch')
    if scope is None: return listings
    start,end=validate_scan_scope(scope)
    selected=[]
    for item in listings:
        # Both inbox and library retain this export timestamp on promotion and
        # replacement. Existing tags have no device/source/session index.
        stamp=item.filename[:16]
        try: exported=datetime.strptime(stamp,'%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc)
        except ValueError:
            raise ValueError('archive has an unclassifiable export name; scope cannot prove coverage') from None
        if start<=exported<end: selected.append(item)
    if len(selected)>scope['max_objects']:
        raise RuntimeError('archive object limit exceeded before download')
    return selected


def discover_objects(store, *, workers=1, selector=None, scan_scope=None, store_factory=None, download_workers=1, progress_every=1000):
    """Read-only/live discovery; only object/line identities and counts survive."""
    counts = {'objects': 0, 'lines': 0, 'matches': 0}
    if not 1<=download_workers<=32: raise ValueError('download workers must be 1..32')
    if download_workers>1 and store_factory is None:
        raise ValueError('parallel downloads require a separate client per thread')
    candidates=[]
    listings=scoped_listings(store,scan_scope)
    local=threading.local()
    def fetch(listing):
        if store_factory is None: reader=store
        else:
            if not hasattr(local,'store'): local.store=store_factory()
            reader=local.store
        return reader.get_object(listing.ref)
    downloaded=0
    with ConfirmationPool(workers, selector) as pool, ThreadPoolExecutor(max_workers=download_workers) as downloads:
        # Keep at most one worker-sized group in memory; executor.map over the
        # full corpus would queue everything and retain downloaded payloads.
        for start in range(0,len(listings),download_workers):
            group=listings[start:start+download_workers]
            for listing,compressed in zip(group,downloads.map(fetch,group),strict=True):
                downloaded+=len(compressed)
                if scan_scope and downloaded>scan_scope['max_bytes']:
                    raise RuntimeError('archive byte limit exceeded')
                raw = gzip.decompress(compressed).decode('utf-8')
                _,matches=_line_matches(raw,pool)
                if not matches:continue
                counts['objects']+=1;counts['lines']+=len(matches);counts['matches']+=sum(matches.values())
                ref={k:v for k,v in listing.ref.items() if k in ('storage_backend','storage_file_id','storage_key')}
                candidates.append({'ref':ref,'lines':list(matches),'matches':sum(matches.values())})
            done=start+len(group)
            # Counts only, on stderr: a bounded scan takes hours and stdout is the result.
            if scan_scope is not None and (done==len(listings) or done//progress_every>start//progress_every):
                print(f"archive scan {done}/{len(listings)} objects {downloaded} bytes "
                      f"{counts['objects']} candidate objects",file=sys.stderr,flush=True)
    plan = {'version':1,'candidates':candidates,'counts':counts}
    if scan_scope is not None:
        plan.update(scan_scope=scan_scope, scanned={'objects':len(listings),'bytes':downloaded})
    if selector and selector.scope != "all": plan.update(selector=selector.specification,left_alone=pool.left_alone)
    return plan


def _validate_object_plan(plan):
    if 'scan_scope' in plan:
        validate_scan_scope(plan['scan_scope'])
        if set(plan.get('scanned',{}))!={'objects','bytes'} or any(type(v) is not int or v<0 for v in plan['scanned'].values()):
            raise ValueError('invalid scanned counts')
    if "selector" in plan:
        from personal_data_warehouse.card_scrub_selector import validate_specification
        validate_specification(plan["selector"])
        if type(plan.get("left_alone")) is not int or plan["left_alone"] < 0: raise ValueError("invalid left-alone count")
    if set(plan)-{"selector","left_alone","scan_scope","scanned"}!={'version','candidates','counts'} or plan['version']!=1:
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
    # replacement pass. --verify performs a fresh scan within the saved scope.
    listings={item.ref['storage_file_id']:item for item in scoped_listings(store,plan.get('scan_scope'))}
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
    parser.add_argument('--exported-from')
    parser.add_argument('--exported-before')
    parser.add_argument('--max-objects',type=int,default=70000)
    parser.add_argument('--max-bytes',type=int,default=5_000_000_000)
    parser.add_argument('--download-workers',type=int,default=16)
    add_selector_arguments(parser)
    args = parser.parse_args()
    baseline=json.loads(args.plan.read_text()) if args.plan and (args.apply or args.verify) else None
    selector = selector_from_args(args,expected=baseline.get('selector') if baseline else None)
    scan_scope=baseline.get('scan_scope') if baseline else None
    if args.exported_from or args.exported_before:
        supplied=dict(exported_from=args.exported_from,exported_before=args.exported_before,
                      max_objects=args.max_objects,max_bytes=args.max_bytes)
        validate_scan_scope(supplied)
        if baseline and supplied!=scan_scope: parser.error('scan scope differs from saved plan')
        scan_scope=supplied
    if args.scope=='tokens' and scan_scope is None: parser.error('token archive work requires an explicit bounded export interval')
    if not 1<=args.download_workers<=32: parser.error('--download-workers must be 1..32')
    if args.workers<1:parser.error('--workers must be positive')
    if args.apply and args.verify:parser.error('--apply and --verify are mutually exclusive')
    if not args.verify and args.plan is None:parser.error('--plan is required')
    settings=load_settings(require_gmail=False, require_agent_sessions=True)
    factory=lambda: _agent_sessions_object_store(settings)
    store = factory()
    if args.apply:
        plan=json.loads(args.plan.read_text())
        scrub_objects(store,apply=True,plan=plan,workers=args.workers,selector=selector)
        print(json.dumps({'baseline':plan['counts'],'verification_pending':True}))
    elif args.verify:
        after=discover_objects(store,workers=args.workers,selector=selector,scan_scope=scan_scope,
                               store_factory=factory,download_workers=args.download_workers)['counts']
        print(json.dumps({'after':after}))
        if args.scope == 'tokens' and after['matches']:
            raise SystemExit('token verification failed: secrets remain')
    else:
        plan=discover_objects(store,workers=args.workers,selector=selector,scan_scope=scan_scope,
                              store_factory=factory,download_workers=args.download_workers)
        _validate_object_plan(plan)
        fd=os.open(args.plan,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
        with os.fdopen(fd,'w') as stream:json.dump(plan,stream)
        print(json.dumps({'before':plan['counts'],'left_alone':plan.get('left_alone'),'scanned':plan.get('scanned')}))


if __name__ == '__main__':
    main()
