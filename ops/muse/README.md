# Muse uploader

`pdw ingest muse` uploads a Muse agent's transcripts and workspace files. It runs on the
Muse VM itself, scheduled by a Muse hook. See the Muse section of `AGENTS.md` for why.

## Install (once per Muse VM)

From a shell on the VM (the home directory, `/home/hatch`, is the only thing that
survives a VM restart, so everything lives there):

1. Put a release `pdw` at `~/.local/bin/pdw` and log it in (`pdw login`). Export
   `PDW_NO_AUTO_UPDATE=1` for interactive use; the self-update check hangs behind the
   egress proxy. Update it with `PDW_NO_AUTO_UPDATE=1 pdw update`.
2. Write the account label, which does not belong in this public repo:
   `printf 'MUSE_ACCOUNT=<account>\n' > ~/.config/pdw/muse.env`.
3. Copy the hook script: `mkdir -p ~/hooks/scripts && cp pdw-ingest-hook.sh
   ~/hooks/scripts/pdw-ingest.sh && chmod +x ~/hooks/scripts/pdw-ingest.sh`.
4. Ask Muse, in a chat, to register it: `hooks.add` with id `pdw-ingest`, script
   `~/hooks/scripts/pdw-ingest.sh`, poll interval 300 seconds, and a worker prompt of
   "Nothing to do; this hook always stays silent." Then `hooks.dry_run` and
   `hooks.enable`. New hooks start disabled.

A first run backfills everything and can take a few minutes; run
`~/hooks/scripts/pdw-ingest.sh` by hand once to watch it. Its log is
`~/.local/state/pdw/muse-upload.run.log`, and `marts_ops.pipeline_health` has the
`muse` row.
