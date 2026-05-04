# Changelog

## Unreleased

- Breaking: redesigned `query` to return cached `query_id` cursors plus preview/truncation metadata. Substring-based SQL pagination for long fields is no longer supported; callers should use `get_field`, `get_rows`, and `grep_rows`.
