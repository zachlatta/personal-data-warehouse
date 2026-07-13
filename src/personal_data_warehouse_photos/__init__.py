"""Photo uploaders: Apple Photos now, Google Photos / manual imports later.

Every photo source posts through the SAME two app endpoints
(``/ingest/photos/file`` + ``/ingest/photos/metadata``) using the shared
envelope contract in :mod:`personal_data_warehouse_photos.envelope`, with its
own ``source`` slug. Serverside, the Dagster reader routes each envelope into
that source's raw table via ``PHOTO_SOURCE_RELATIONS``
(:mod:`personal_data_warehouse.relations`), and the identity layer dedups
renditions into logical photos.
"""
