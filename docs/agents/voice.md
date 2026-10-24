# Voice recordings

Moved out of AGENTS.md on 2026-10-01 so the file every session loads holds only the
contracts and the rules every change needs. Start at [AGENTS.md](../../AGENTS.md).

## Voice recordings (a multi-source domain, one pipeline)

Voice has **four** sources — `base_apple_voice_memos.files` (the Mac uploader),
`base_google_recorder.recordings` (Google Recorder on the Pixel, from 2026-10-23),
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

**A short memo is a stored transcript ~30 s after it reaches the Mac, and that is measured.**
Measured 2026-10-16 over 30 days of Apple memos, end of recording to transcript was p50
9 min, and almost none of it was work: a 120 s file-age gate and the uploader's five-minute
tick, a 60 s inbox sensor and a Dagster run start, a 60 s transcription sensor and another
run start, then the five-minute `timeline_sync`. AssemblyAI itself took 7 s on a short memo
(18 s for 2-15 min, 41 s past 15 min). Now: the LaunchAgent fires on the store's WAL and the
Recordings directory (10 s throttle), the run waits out a 5 s settle itself, the app NOTIFYs
`pdw_voice_memo_landing` when it stores a sidecar, and the resident
`voice_memo_landing_worker` in the Dagster container runs the same ingest and transcription
functions the assets run (`ingest_voice_memos_inbox`, `transcribe_voice_recordings`), then the
timeline fast lane. AssemblyAI is polled every second for the first 30 s. Every voice asset
(ingest, transcription, enrichment) also lands what it wrote through the fast lane. The
sensors stay as the floor and the inbox is the durable queue, so a lost notification costs
latency, never a recording; `VOICE_MEMO_LANDING_WORKER_ENABLED=0` turns the worker off.
`marts_ops.voice_memo_landing_health` judges PDW's part (Mac file to transcript, memos under
two minutes) against a 30 s p50 and reports iCloud's phone-to-Mac delivery beside it, never
in the status: that leg is Apple's (p50 17 s, p90 10 min for short memos, 36 min once for a
20-minute memo). Semantic search still trails by the chunk (5 min) and embedding (10 min)
schedules; keyword search reads the timeline row at once.

**`recorded_at` is the store's ZDATE, not the filename.** The filename stamp
(`20261015 205541-…`) is the recording device's wall clock in an unrecorded zone, and until
2026-10-16 the uploader parsed it as UTC, so every Apple memo was stored 4-5 hours early (231
of 262 calendar matches started ~4 h after the stored time; the file reached the Mac 4 h plus
the memo's length after it). Calendar matching survived only because enrichment searched both
readings, and that hedge assumed New York, so it broke on travel; it is gone. The uploader
reads `ZCLOUDRECORDING.ZDATE` (seconds since 2001-01-01 UTC), falling back to the filename in
the Mac's zone. The state file records the `recorded_at` each sidecar carried, and a landed
memo whose value changed gets a corrected sidecar only (no audio, no re-hash), 150 per run.
The app dedups a sidecar on (audio sha, `recorded_at`), not the audio sha alone, or the
correction would be dropped as a duplicate, and the Drive reader finds a corrected sidecar's
audio in the library. Memos no longer on the Mac keep the old, early time.
Voice Memos also renames files now and then (the stamp moves by hours; one was named in
Pacific time and renamed in Eastern), and the recording id is the stem. The old audio-hash
dedup had silently absorbed those renames, so the first correction pass landed three renamed
memos as new recordings, transcribed them again and queued them for enrichment; they were
removed by hand the same day. The Drive reader now keeps the recording id the warehouse
already holds for that audio (`_keep_known_recording_ids`), never for empty audio.

**A hung upload run is killed after 15 minutes.** On 2026-10-14 a run hung at "Scanning" for
10.9 hours, and launchd never starts a job that is still running, so two memos waited ~13
hours. `--max-run-seconds` (default 900 incremental, off for `--mode full`) exits with 124,
which the heartbeat reports as a failed run.

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
mishearing is one tuple. A prompt version bump re-enriches nothing on its own: a recording
with any completed enrichment is not a candidate again unless the Dagster deployment sets
`VOICE_MEMOS_ENRICHMENT_FORCE_PROMPT_VERSION=1`, which re-enriches the whole corpus.
For a bounded backfill set `VOICE_MEMOS_ENRICHMENT_FORCE_PROMPT_VERSION_SINCE` to an ISO
date instead: only recordings made on or after it are re-enriched under the current prompt,
newest first, and they drop out of the queue once done, so the variable can stay set. Set to
`2026-09-14` on 2026-10-14 to bring the last 30 days (96 recordings) onto prompt v11
(`transcript_corrections`) after the "Gemma" for Jehn miss in the 2026-10-12 Musk
Foundation recording.

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

**Speakers: a wider ceiling for long recordings, and turn-level names from the agent.**
`max_speakers_expected` is a hard ceiling -- AssemblyAI merges every speaker past it into
an existing label -- so `assemblyai_speaker_options()` keeps the configured 8 for a
conversation and lifts it to 30 (AssemblyAI's own long-audio default) for a recording of
30 minutes or more. Diarization still merges stage speakers: the 5.7-hour stage-talks
recording of 2026-10-03 had ten identified speakers in five labels. So the enrichment
agent (prompt `...-agent-v8`) returns `speaker_turns` beside `speaker_map`: contiguous
`segment_index` ranges, each with one verified person, found from on-stage introductions,
handoffs and the event agenda. Local transcript assembly applies a turn at confidence
0.9 or more ahead of the label mapping, and `validate_enrichment_result` rejects reversed,
overlapping or out-of-range turns. A label the agent calls mixed or unresolved now stays
that way in the assembled transcript. The assembler used to read a name out of the label's
evidence ("mostly consistent with X"), which is how 302 turns of that event landed under
the organizer's name.

**A new transcript means a new enrichment.** An enrichment counts as done only when it was
created after the transcription run it read (`e.created_at >= r.completed_at` in
`load_enrichment_candidates`). Re-transcribing a recording therefore re-enriches it with
no prompt-version bump. The runner replaces the recording's segments
(`replace_voice_recording_transcript_segments`) instead of upserting over them, so a
shorter new transcript leaves no tail of the old one for the agent to read. The rule is about wall
time, so the runner also checks, just before writing, that the recording still has the
transcript the candidate was read from (`provider_transcript_id`); a run that started on
the old segments of a recording re-transcribed meanwhile writes nothing instead of landing
after the new transcript as if it were current (2026-10-04: an enrichment started at 17:28,
the re-transcription finished at 17:35).

**The ceiling, measured.** On 2026-10-04 the stage-talks recording was hand-labelled -- 14
identifiable speakers, 82 anchor moments whose speaker is certain -- and re-diarized under
several `speaker_options`, scored as pairwise "same person iff same label" F1:

| speaker_options | labels | pairwise F1 |
| --- | --- | --- |
| max 8 (the old ceiling) | 5 | 0.39 |
| max 30 | 14 | 0.84 |
| min 10, max 30 | 14 | 0.84 |
| max 50 | 13 | 0.80 |
| min 16, max 30 | 18 | 0.71 |
| min 20, max 40 | 19 | 0.69 |
| omitted entirely | 4 | 0.33 |

More room helps, forcing a floor hurts, and leaving `speaker_options` out does NOT get the
documented default. A 45-minute, twelve-person breakout from the day before came back as
**one utterance from one speaker** at max 8 and 108 utterances from 16 speakers at max 30.
The one merge max 30 still makes -- an interviewer and her guest on the same stage
microphones, one label and one 12-minute utterance -- survives every setting, so an
utterance longer than 20 seconds is stored one sentence per segment (`split_long_utterance`,
same label): a `speaker_turns` range can only split people at a segment boundary. Local
assembly merges a speaker's consecutive segments back into one paragraph, and
`segment_speaker_names` is the one place that decides each segment's printed name (the
benchmark harness scores through it).

**A `speaker_turns` entry may be scoped to one label.** With `speaker_label` set, an entry
names only that label's segments inside its range -- one entry says "label J is the guest for
the whole interview". With it empty, the entry is an exact turn and outranks a label-scoped
one, which is how a question on the guest's label goes to the interviewer. Without scoping,
covering a 3,000-segment recording meant listing every turn: one run left 2,299 segments
unresolved, including a whole 35-minute interview.

**The enrichment agent runs on GPT-6 Astra, not the fleet model.**
`DEFAULT_VOICE_MEMOS_ENRICHMENT_MODEL` is `gpt-6-astra`; `VOICE_MEMOS_ENRICHMENT_MODEL` /
`VOICE_MEMOS_ENRICHMENT_REASONING_EFFORT` override it per deployment, and the client builds
its own agent resource because the injected fleet one carries `AGENT_MODEL`. Neither the
model nor the prompt version is part of an enrichment's completion identity (unless
`VOICE_MEMOS_ENRICHMENT_FORCE_PROMPT_VERSION=1`), so the switch applies to new recordings and
to re-transcribed ones; re-enriching all ~775 existing recordings on Astra was judged not
worth ~40 hours of serial agent runs, most of them over the old 8-speaker diarization.

**A long recording is several sessions; the prompt (`...-agent-v10`) attributes per session.**
The agent splits the recording into sessions from introductions and handoffs, writes one
`speaker_turns` range per question and per answer when two people share a label, names a
speaker introduced by first name only by that first name rather than leaving every turn
unresolved (participants still need full names), and names the recording owner's
identifiable turns in side conversations. Every label that speaks for a minute or more must
be in `speaker_map`, and a recording of 20 minutes or more gets a sectioned summary sized to
its length (`summary_length_issues`). On the benchmark, the v7/Sol enrichment over the
8-speaker diarization named the WRONG person at 55 of 82 anchors -- every line the MC spoke
was attributed to Zach, every question the interviewer asked to her guest -- and summarized
5.7 hours in 720 characters. Over the max-30 diarization, v10 (sentence segments, label-scoped turns) got
73 of 82 right, zero wrong, the remaining nine almost all an interviewer nobody names;
v9 got 67 of 82 right with **zero** wrong names (a prototype of the same rules: 70 and 69), the rest
unresolved -- an interviewer nobody names, segments holding both a question and its answer,
and the owner's reception turns, the one gap still open -- with a 17,000-character sectioned
summary, in about ten minutes a run. Wrong names are worse than unresolved ones; that is the number to watch when this
prompt changes. The harness is `scripts/voice_memo_speaker_benchmark.py`; its hand labels
name real people and live outside the repo in `~/.config/pdw/voice-benchmark/`.

**The enrichment error budget is per transcript.** `max_error_attempts` counts failures
newer than the recording's current transcription run, not every failure ever: two
recordings that failed five times in April and then succeeded were shut out of the queue
for good when they were re-transcribed to repair a corrupted transcript on 2026-10-04.

**Name canonicalization rewrote domain words for months.** After the agent returns, a fuzzy
pass (`canonicalize_text_verified_name_mentions`) corrects near-miss spellings of verified
names everywhere in the result. Its only guard was string similarity, so a "name" one edit
away from a common word rewrote that word everywhere: an audit on 2026-10-04 found 27
recordings whose enriched transcript had lost every "Hack Club" (to "Hacker Club", "Jack
Club", "Hackle Club", "Zack Club"), 12 that lost "OpenAI", and "Congressional App Challenge"
stored as "Congressional App Charles". The "names" came from a participant spelled Zack, a
participant named Charles, and ASR variants the agent had named in its evidence ("the ASR
rendered Hacker Club"), which local assembly harvests as people. The pass now never touches
a word of a known domain term (`protected_domain_words`, from the keyterms and the confusion
hints), a word the same text also uses in lowercase, or a different first letter unless it
makes the same sound (Kristen/Cristen). The source transcript was never affected; repair is
re-enrichment.

**Validation issues never reach the agent.** `validate_enrichment_result` runs after the
agent returns, and its issues are stored on the row (`raw_result_json.__validation_issues`)
rather than sent back for a retry, so a check there is a measurement, not a guard. The
agent sees only the JSON schema (`$PDW_VALIDATE_JSON`). A rule that must hold belongs in the
prompt or in local post-processing.

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

**A memo stopped and restarted is one event: `marts_voice_memos.sessions`.** Added
2026-10-20 at Zach's request ("if I stop a memo and then start another one, it'll cleanly be
able to transition them over"; it must understand the content, not guess from timing). The
`voice_memo_continuity` asset runs after enrichment (hourly :47 plus a two-minute backlog
sensor; `VOICE_MEMO_CONTINUITY_ENABLED=0` turns it off). Step one: every pair of consecutive
recordings of one source and account that sit within 30 minutes
(`VOICE_MEMO_CONTINUITY_MAX_GAP_SECONDS`) goes to the fleet agent with the end of the first
transcript, the start of the second and both summaries; it answers `same_event` or `separate`
with a confidence and reason, stored per pair in `derived_voice_memos.continuity_judgments`
and asked again when either transcript changes. Time only proposes: measured over 60 days,
the window holds every real restart (a lecture split by 3 s, a demo session by 8 s) and
dozens of unrelated task memos a minute apart. Step two: a run of recordings joined by
`same_event` at confidence 0.7 or more is a session, and a session of two or more gets one
enrichment written over all its members (`derived_voice_memos.session_enrichments`, keyed by
the member list, redone when a member is re-enriched). A session that ended inside the window
with nothing recorded after it waits, so three restarts merge once. The view gives every
recording exactly one session (a lone recording is a session of one with its own
enrichment); `merge_status` is `single`, `pending` (joined, merged record not written yet:
the first member's title stands in) or `merged`. Each member recording keeps its own row in
`marts_voice_memos.recordings` and on the timeline; sessions are not a timeline source yet.

## Google Recorder on the Pixel

Zach carries a Pixel from 2026-10-23, so new memos are Google Recorder recordings (a
press and hold of the power button starts and stops one; sysadmin
`pichu/2026-10-23-power-button-voice-memo.md`). Recorder keeps its recordings and
its `recording_database` in its private data directory, and the phone is rooted, so the
uploader runs **on the phone, as root**: `pdw ingest google-recorder`, the release
linux/arm64 build, started by the `pdw-google-recorder` Magisk module
(`ops/android/pdw-google-recorder/service.sh`, wrapper `bin/google-recorder-upload-android`,
deployed by the sysadmin repo's `scripts/install-pixel-recorder-upload`). Nothing waits on
a Mac or on iCloud: from the end of a recording every second is PDW's, and
`marts_ops.voice_memo_landing_health` judges `google_recorder` from the end of the
recording against the same 30 s p50.

- **What it sends.** Each recording whose `recording_state` is 3 (finished), as the
  `.m4a` Recorder wrote plus a sidecar: Recorder's uuid (`recording_id`), title,
  `created_time` (epoch milliseconds, UTC, so there is no zone to guess, unlike the Apple
  filename stamp), duration in ms, place name and coordinates, account and device. The app
  files both under `google-recorder/inbox/` (`/ingest/google-recorder/audio/resumable`,
  `/ingest/google-recorder/metadata`, which NOTIFYs the voice landing worker), and
  `google_recorder_drive_inbox_sensor` / the landing worker promote them into
  `base_google_recorder.recordings`. From there the mart, transcription, enrichment,
  continuity and the timeline treat it like every other source.
- **It never opens Recorder's live database.** Recorder holds it in WAL mode under its
  own uid; a root reader that created the `-shm` or `-wal` beside it would leave a file
  Recorder cannot write. The uploader copies the database and its WAL (not the `-shm`)
  to its own scratch directory, runs `quick_check` on the copy and reads that.
- **A rename sends a new sidecar; an edit sends the new audio.** The ledger
  (`/data/misc_ce/0/pdw-google-recorder/google-recorder-upload-state.sqlite`) keeps, per uuid,
  the audio sha, the file stat it was taken at and the sidecar's dedup sha (the sidecar
  without `uploaded_at`), and the app dedups on the same shas. The row's `sync_version` is
  the sidecar's upload time, so the newest sidecar wins. A recording deleted in Recorder
  is NOT tombstoned yet: its row stays.
- **Triggered, and awake.** `inotifyd` watches Recorder's `databases/` and
  `files/recordings/`; each write marks a run due and takes a 60 s auto-expiring kernel
  wakelock, because a memo stopped with the screen off is otherwise followed by suspend
  within a second or two and the upload would wait for the next wake. Runs are at least
  10 s apart (Recorder writes while it records) and every 5 minutes as the floor; each
  run holds its own wakelock and is cut at 960 s. The heartbeat (`google_recorder` in
  `ops.uploader_heartbeats`) is posted on a failure, on a run that sent something, and
  otherwise at most every four minutes. It uploads on any network: a memo is ~22 MB an
  hour, and the screen timelapse's Wi-Fi-only rule exists for frames, not memos.
- **DNS and TLS roots.** Android has no `/etc/resolv.conf`, so Go's resolver asked
  `[::1]:53` and every lookup failed; and Android 14+ keeps its trusted roots in the
  Conscrypt APEX, which Go's Linux certificate list does not name, so every request then
  failed "certificate signed by unknown authority". `internal/androidnet` (called first
  in pdw's `main`, a no-op elsewhere) asks 1.1.1.1, 8.8.8.8 and MagicDNS in turn when
  that file is missing (`PDW_DNS_SERVERS` overrides) and sets `SSL_CERT_DIR` to
  `/apex/com.android.conscrypt/cacerts` and `/system/etc/security/cacerts`.
- **The upload guard is the app preflight only** (`common.PreflightGuard`). The Mac
  uploaders' tethering policy blocks cellular, which is the phone's normal network, and
  Android's policy routing leaves `/proc/net/route` with no default route, so the full
  guard deferred every run.
- **The state directory is credential-encrypted** (`/data/misc_ce/0/pdw-google-recorder`):
  its `config/pdw/config.json` holds the warehouse master secret (ingest uploads are
  HMAC-signed with it), and with the bootloader unlocked `/data/adb` is readable before
  the PIN is entered. Before the first unlock after a reboot neither it nor Recorder's
  storage exists, and the service waits for both. An OTA that drops root stops the uploader with
  everything else that needs root (sysadmin `pichu/README.md`).

On the phone (`ssh pichu`): logs and heartbeat in
`/data/misc_ce/0/pdw-google-recorder/logs/`, `touch /data/misc_ce/0/pdw-google-recorder/paused` to
stop uploads, and a manual run is
`sh /data/adb/modules/pdw-google-recorder/bin/google-recorder-upload-android`.

## Local Voice Memos Upload Scheduler

This Mac is intended to run the local Voice Memos uploader through a user LaunchAgent:

- LaunchAgent label: `com.zachlatta.personal-data-warehouse.voice-memos-upload`
- Installed plist: `~/Library/LaunchAgents/com.zachlatta.personal-data-warehouse.voice-memos-upload.plist`
- Checked-in plist template: `ops/launchd/com.zachlatta.personal-data-warehouse.voice-memos-upload.plist`
- Wrapper script: `bin/voice-memos-upload-launchd`
- Run cadence: on every change to `CloudRecordings.db-wal` or the Recordings directory
  (`WatchPaths`, 10 s `ThrottleInterval`), and every 300 seconds with `RunAtLoad` as the floor
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
