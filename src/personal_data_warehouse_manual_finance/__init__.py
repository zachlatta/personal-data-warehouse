"""Manual finance document uploader (`pdw ingest manual-finance`).

Uploads bank/mortgage statements, valuation screenshots, fund position docs,
and transaction exports through the app's /ingest/manual-finance endpoints.
The uploader's folder-per-account organization is preserved as the
`original_path` envelope field and the object key's account segment.
"""
