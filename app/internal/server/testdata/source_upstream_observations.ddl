pipeline text NOT NULL DEFAULT ''
scope text NOT NULL DEFAULT ''
upstream_newest_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz
upstream_newest_id text NOT NULL DEFAULT ''
detail text NOT NULL DEFAULT ''
observed_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz
pending_since timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz
updated_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz
sync_version bigint NOT NULL DEFAULT 0
PRIMARY KEY (pipeline, scope)
