"""Explicit scrub of archived/inbox conversation batches (dry-run by default).

Replacement uploads contain only sanitized bytes. Verify replacement bytes before
deleting the original object. Stop writers and batch consumers while applying.
"""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import tempfile

from personal_data_warehouse.card_redaction import card_count, redact


def scrub_objects(store, *, apply=False):
    counts = {'objects': 0, 'lines': 0, 'matches': 0}
    for listing in store.list_objects(kind='agent_sessions_export_batch'):
        raw = gzip.decompress(store.get_object(listing.ref)).decode('utf-8')
        output = []
        changed = False
        for line in raw.splitlines(keepends=True):
            n = card_count(line)
            if n:
                changed = True
                counts['lines'] += 1
                counts['matches'] += n
                output.append(redact(line.rstrip('\n')) + ('\n' if line.endswith('\n') else ''))
            else:
                output.append(line)
        if not changed:
            continue
        counts['objects'] += 1
        if not apply:
            continue
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
    return counts


def main():
    from personal_data_warehouse.config import load_settings
    from personal_data_warehouse.defs.agent_sessions_drive_ingest import _agent_sessions_object_store
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    store = _agent_sessions_object_store(load_settings(require_gmail=False, require_agent_sessions=True))
    print(json.dumps({'before': scrub_objects(store)}))
    if args.apply:
        scrub_objects(store, apply=True)
        print(json.dumps({'after': scrub_objects(store)}))


if __name__ == '__main__':
    main()
