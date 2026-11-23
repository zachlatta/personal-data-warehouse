"""Scrub local JSONL copies; default dry-run, never prints paths or content."""
import argparse
import json
import os
from pathlib import Path
import tempfile

from personal_data_warehouse.card_redaction import sensitive_count, redact
from personal_data_warehouse.card_scrub_selector import add_selector_arguments, selector_from_args


def scrub_jsonl(root: Path, session: str, *, apply=False, selector=None):
    counts = {'files': 0, 'lines': 0, 'matches': 0}
    if selector and selector.scope != "all": counts["left_alone"] = 0
    for path in root.rglob('*.jsonl'):
        if path.is_symlink():
            continue
        # Preserve line endings as well as JSON lexemes during replacement.
        original = path.read_bytes().decode('utf-8')
        output = []
        changed = 0
        for line in original.splitlines(keepends=True):
            selected = session in str(path.relative_to(root))
            if not selected:
                try:
                    parsed = json.loads(line)
                    selected = isinstance(parsed, dict) and parsed.get('sessionId') == session
                except ValueError:
                    pass
            n = (selector.count if selector else sensitive_count)(line) if selected else 0
            if selected and "left_alone" in counts: counts["left_alone"] += sensitive_count(line) - n
            if n:
                counts['matches'] += n
                counts['lines'] += 1
                changed += 1
                newline = '\n' if line.endswith('\n') else ''
                output.append((selector.redact if selector else redact)(line.rstrip('\n')) + newline)
            else:
                output.append(line)
        if changed:
            counts['files'] += 1
            if apply:
                fd, name = tempfile.mkstemp(dir=path.parent, prefix='.scrub-')
                try:
                    os.fchmod(fd, path.stat().st_mode & 0o777)
                    with os.fdopen(fd, 'w') as stream:
                        stream.write(''.join(output))
                        stream.flush()
                        os.fsync(stream.fileno())
                    os.replace(name, path)
                finally:
                    if os.path.exists(name):
                        os.unlink(name)
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--session', required=True)
    parser.add_argument('--apply', action='store_true')
    add_selector_arguments(parser)
    args = parser.parse_args()
    selector = selector_from_args(args)
    counts=scrub_jsonl(args.root,args.session,apply=args.apply,selector=selector)
    print(json.dumps(counts))
    if args.scope=='credentials':
        remaining=scrub_jsonl(args.root,args.session,selector=selector)['matches'] if args.apply else counts['matches']
        if remaining: raise SystemExit('credential verification failed: secrets remain')


if __name__ == '__main__':
    main()
