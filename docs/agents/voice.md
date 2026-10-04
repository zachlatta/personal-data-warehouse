# Voice recordings

Moved out of AGENTS.md on 2026-10-01 so the file every session loads holds only the
contracts and the rules every change needs. Start at [AGENTS.md](../../AGENTS.md).

## Voice recordings (a multi-source domain, one pipeline)

Voice has **three** sources — `base_apple_voice_memos.files` (the Mac uploader),
`base_alice_voice_recordings.recordings`, and the audio rows of
`base_apple_notes.attachments` (call recordings, voicemails and the Notes app's own
recorder; collapsed to one recording per attachment on its newest revision) — and exactly
one of everything downstream.
**Start at `marts_voice_memos.recordings`**: one row per recording from either source,
with the resolved title, summary, transcript, participants, action items and calendar
match as real columns. `marts_voice_memos.transcript_segments` holds the speaker-labelled
utterances.

```sql
SELECT source, recorded_at, title, summary
FROM marts_voice_memos.recordings
WHERE transcript IS NOT NULL ORDER BY recorded_at DESC LIMIT 20;
```

**A provider rejection is the transcription pipeline's state, and it reads `failing`.**
`derived_voice_memos.transcription_runs` records every AssemblyAI rejection as
`status = 'error'` with the response text, and `voice_memo_transcription` declares it as its
`StateSource`. On 2026-08-27 every call had returned `400 … account balance is negative` for
hours while the row read green and 45 recordings across three sources sat untranscribed,
attributed to a quiet uploader. A successful retry overwrites the error row, so the failure
clears itself; a persistent one is a billing or credential action, not a pipeline bug.

**A history table needs an `error_window`; a current-state table must never have one.**
`StateSource` was built for `ops.*_sync_state`, where ONE upserted row per scope IS that
scope's state today -- ageing a row out there would hide a live outage, which is why
`whoop` (26 hours hard-down reading `ok` once) has no window. `transcription_runs` is the
opposite shape: one row per attempt, never revisited. Measured 2026-08-27 after the
`rejected` split landed, two rows from **2026-05-02** still pinned the pipeline red --
`"Upload failed, please try again"`, a genuinely retryable message so correctly not
`rejected`, on recordings of `size_bytes = 0`, which the candidate query excludes, so
nothing would ever retry them and clear it. A live outage re-stamps `requested_at` on
every run and stays inside the window; an error that has stopped being re-stamped is
history, not state. Seven days, and the reported reason is windowed with the count so the
banner cannot quote a failure that no longer colours the row.

**A recording the provider will never accept is `rejected`, not `error`, and that
distinction is what keeps the row readable.** The error count behind
`state_error_rows` is over the WHOLE runs table with no time bound, so a single
permanently unacceptable recording pins `voice_memo_transcription` to `failing`
forever. Production had eleven of them -- "no spoken audio", "audio duration is too
short", "does not appear to contain audio", oldest 2026-05-01 -- which means the row
was **already red** when the balance outage arrived on 2026-08-27 and the StateSource
added to catch that outage could not have caught it. `rejected` is terminal for the
candidate query and sits outside `StateSource.error_statuses`, exactly as slack's
`gone` does. The classifier is an **allow-list** of recognised rejections
(`PERMANENT_VOICE_MEMO_TRANSCRIPTION_REJECTION_PATTERNS`), never "whatever is not
retryable": mistaking a permanent rejection for a transient one costs one wasted API
call, while mistaking a TRANSIENT failure for permanent silently retires the recording
and hides the outage. An unrecognised error stays `error` and stays red on purpose.

**"Out of credits" was the wrong account, and the balance alone cannot tell you which.**
The 2026-08-27 rejection was real, but it belonged to an AssemblyAI account whose balance
had gone negative -- while the account Zach was reading a positive balance on was a
*different* one, whose key production did not hold. Both statements were true at once, so
the disagreement is not evidence that the API is lying. The check that settles it takes one
call and names the account: post a transcript with the key production actually has and read
`project_id` off the response. Rotating the key is the repair, and it is only complete when
the value in the Coolify **Dagster** deployment changes -- the app deployment carries no
`ASSEMBLYAI_API_KEY`, so updating the repo `.env` alone fixes nothing in production.

**The speech model mishears "Hack Club" as "HackPad", and the transcript is left
alone on purpose.** Universal-3.5 Pro produces `HackPad` on some audio even though
`Hack Club` sits in `keyterms_prompt`, and AssemblyAI's `custom_spelling` cannot repair
it -- the API rejects a multi-word `to` field, and "Hack Club" is two words. Measured
2026-08-27 across seven recordings re-run through both models: `Hack Club` 29 -> 23 and
`HackPad` 0 -> 3, with **all** of the loss inside one acoustically hard recording and
five of seven preserving the term exactly. The corpus says which one is real -- 504 of
632 stored transcripts contain `Hack Club`, 12 contain `HackPad` -- so a `HackPad` hit
is almost always the mishearing rather than the defunct Dropbox product.

**No stored transcript is unreachable by this today, and that is a fact with a shelf
life.** Measured 2026-08-27, every one of the 12 transcripts containing `HackPad` also
contains `Hack Club` somewhere else, so a `Hack Club` search currently misses **zero**
recordings. What the mishearing costs is a *count*, not a document -- until a short
recording arrives whose every mention is misheard, at which point that one really does
become unreachable. Search both spellings when the answer depends on completeness. The repair is
deliberately NOT a rewrite of `transcript`: editing the provider's words would destroy
the evidence that the mishearing happened and would corrupt a genuine mention. Instead
`ASR_CONFUSION_HINTS` in `apple_voice_memos_enrichment.py` carries the (heard, intended,
why) triple, `enrichment_system_prompt()` hands it to the enrichment agent, and the
agent resolves the term in `title`, `summary`, `participants` and `action_items` while
leaving `transcript` untouched and recording what it did in `evidence`. Adding a future
mishearing is one tuple. The prompt version bump (`...-agent-v7`) re-enriches within
`VOICE_MEMOS_ENRICHMENT_LOOKBACK_WEEKS` rather than the whole corpus.

**Universal-3.5 Pro is the model, with Universal-2 only as the language fallback.**
`ASSEMBLYAI_SPEECH_MODELS` is `("universal-3-5-pro", "universal-2")`, sent as the
`speech_models` fallback chain -- AssemblyAI's own default, where Universal-2 serves only a
language 3.5 Pro does not cover. Universal-3 Pro was dropped from the chain on 2026-10-04
once AssemblyAI stopped listing it. An unknown slug is a 400 that names the valid list, so
a typo fails loud instead of quietly transcribing at a lower quality. `speech_model_used`
on the response records which one ran, and it is what
`derived_voice_memos.transcription_runs.model` stores -- read that rather than assuming
the head of the chain served the request. The marts expose it too:
`marts_voice_memos.recordings.transcript_model` / `provider_transcript_id` for the
transcript the mart serves, and `marts_voice_memos.transcript_segments.transcript_model`
for the run that produced each speaker-labelled segment. The rest of the request
(speaker options, keyterms, language detection) is in that run's `raw_result_json`.

**That mart is the INPUT to transcription and enrichment, not only an output.** Both
passes (`defs/apple_voice_memos_transcription.py`, `defs/apple_voice_memos_enrichment.py`)
take their candidates from it, so a new voice source is transcribed and enriched by
existing code the day its raw table lands. They used to scan
`base_apple_voice_memos.files` by name, and the mart hardcoded `NULL` transcript/summary
for the other branch, which made the NULLs self-fulfilling: Alice sat at 53 recordings, 0
transcripts and 0 summaries while every ENFORCED registry passed. See C5.

**A recording whose audio never landed is asked for again, not skipped.** The Alice poller
archives the metadata sidecar even when the media download fails (deliberately — the
recording is at least known), and until 2026-09-09 that sidecar alone was the incremental
skip test, so a recording that failed once was never downloaded again: production held 8
of 53 Alice recordings at `size_bytes = 0` with no audio object while every daily poll read
green and the contract audit graded them "untranscribable". The skip now requires BOTH the
audio object and the sidecar; a `size_bytes = 0` row with a live source id is a fetch
failure to investigate, never a quiet source.

The three derived tables — `derived_voice_memos.transcription_runs`, `.transcript_segments`
and `.enrichments` — are keyed by **`source` first**, because a `recording_id` is unique
only inside its own source. Without that column a second source's run upserts onto the
first source's row, so the domain could not have stored a second transcript even if
something had produced one.

One timeline adapter (`voice_memo`, source `voice_memos`, kind `voice_memo`, priority
`self`) covers every source over the mart; `timeline.events.metadata->>'voice_source'`
says which one, and `source_pk` carries `{source, account, recording_id}`. Search scopes
them together under `sources => ARRAY['transcript']`.

## Local Voice Memos Upload Scheduler

This Mac is intended to run the local Voice Memos uploader through a user LaunchAgent:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.voice-memos-upload`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.voice-memos-upload.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.voice-memos-upload.plist`
- Wrapper script: `bin/voice-memos-upload-launchd`
- Run cadence: every 300 seconds with `RunAtLoad`
- Command: `pdw ingest voice-memos --mode incremental` (native Go in the signed pdw binary; the wrapper runs nothing else — no uv, no Python)
- Main run log: `~/Library/Logs/personal-data-warehouse/voice-memos-upload.run.log`
- Heartbeat file: `~/Library/Logs/personal-data-warehouse/voice-memos-upload.heartbeat`
- Status helper: `bin/voice-memos-upload-status`

Each run also performs the **enriched-title write-back**: memos that still carry an
app-assigned name ("New Recording N" / geocoded location names — detected by the
`0x1000` auto-named bit in `ZFLAGS`, or the literal `New Recording N` pattern for
pre-flag-era rows) are renamed in the Voice Memos app to the newest completed
`derived_voice_memos.enrichments` title. Hand-typed titles are never overwritten (the
gate is enforced at plan time and re-checked inside the write transaction). The rename
is a proper Core Data save against `CloudRecordings.db` by a small Swift helper
(`app/internal/uploaders/voicememos/macos/VoiceMemosWriteback.swift`, embedded in the pdw
binary and compiled on demand with `swiftc`, the same arrangement as the photos exporter;
driven by `writeback.go` + `storewriter.go`): the model comes from the store's own
`Z_MODELCACHE`, migration is disabled (incompatible future stores fail loudly), and the
save records persistent history under the author
`com.zachlatta.pdw.voice-memo-writeback`, which `voicememod` exports to CloudKit so the
rename syncs to all devices. Kill switch: `VOICE_MEMOS_WRITEBACK_ENABLED=0`. Manual
runs: `pdw ingest voice-memos --writeback-only [--writeback-dry-run] [--writeback-limit N]`,
`--no-writeback` for upload-only. Titles are fetched from the app's `/api/tools/sql`
endpoint with the same `PDW_API_URL`/`PDW_SECRET_TOKEN` the uploader already uses.

Use these commands when inspecting or repairing it:

```bash
bin/voice-memos-upload-status
launchctl print gui/$(id -u)/com.zachlatta.personal-data-warehouse.voice-memos-upload
launchctl kickstart -k gui/$(id -u)/com.zachlatta.personal-data-warehouse.voice-memos-upload
tail -80 ~/Library/Logs/personal-data-warehouse/voice-memos-upload.run.log
cat ~/Library/Logs/personal-data-warehouse/voice-memos-upload.heartbeat
```

If the plist changes, reinstall it with:

```bash
cp ops/launchd/com.zachlatta.personal-data-warehouse.voice-memos-upload.plist ~/Library/LaunchAgents/
launchctl bootout gui/$(id -u)/com.zachlatta.personal-data-warehouse.voice-memos-upload 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.voice-memos-upload.plist
launchctl enable gui/$(id -u)/com.zachlatta.personal-data-warehouse.voice-memos-upload
```

Do not replace this with cron unless there is a specific reason. On current macOS, LaunchAgents
behave better for user-session jobs and are easier to inspect with `launchctl`.

If the run log shows `PermissionError: [Errno 1] Operation not permitted` for
`~/Library/Group Containers/group.com.apple.VoiceMemos.shared/Recordings`, the LaunchAgent is
loaded correctly but macOS Full Disk Access is blocking the background process. Grant Full Disk
Access to the executable chain used by the job: `/bin/zsh` and the `pdw` binary
(`~/.local/bin/pdw`). Nothing else is in the chain any more — no `uv`, no venv python, and
therefore no uv-python path drift to re-check. The grant survives `pdw update` because release
binaries are signed with a stable identity; if it breaks anyway, `codesign -d --verbose=2
~/.local/bin/pdw` must show `Identifier=com.zachlatta.pdw` (a local `go build` is ad-hoc signed
and does NOT inherit the grant — install a release with `pdw update --force`). Then kickstart
the LaunchAgent again.
