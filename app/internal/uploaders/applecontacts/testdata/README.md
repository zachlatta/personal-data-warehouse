# Apple Contacts golden fixtures

`synthetic.abcddb` and `coredata.abcddb` are small SQLite stores in the two
shapes the scanner reads (the synthetic test schema, and Apple's Core Data
`ZABCDRECORD` layout with the real column names). `golden.jsonl` holds, per
card, the canonical JSON payload and sha256 fingerprint the **Python**
uploader (`personal_data_warehouse_apple_contacts`, removed when `pdw ingest`
became native Go) produced for those stores.

The Go test asserts its payload bytes and fingerprints are identical, because a
one-byte difference would re-upload every card on the first native run.
