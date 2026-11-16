"""Scrub local JSONL copies; default dry-run, never prints paths or content."""
import argparse
import json
import os
from pathlib import Path
import tempfile

from personal_data_warehouse.card_redaction import card_count, redact


def scrub_jsonl(root: Path, session: str, *, apply=False):
    counts = {'files': 0, 'lines': 0, 'matches': 0}
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
            n = card_count(line) if selected else 0
            if n:
                counts['matches'] += n
                counts['lines'] += 1
                changed += 1
                newline = '\n' if line.endswith('\n') else ''
                output.append(redact(line.rstrip('\n')) + newline)
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
    args = parser.parse_args()
    print(json.dumps(scrub_jsonl(args.root, args.session, apply=args.apply)))


if __name__ == '__main__':
    main()
