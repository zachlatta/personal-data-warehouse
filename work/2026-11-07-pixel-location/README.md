# Pixel approximate location implementation

Adds a coarse network-provider helper, root scheduling and durable offline queue to the
existing PDW runtime. Authenticated batches retain measurement time, accuracy and server
receipt time. Warehouse integration includes raw source samples, a noise-tier timeline
adapter, measured-time health and agent source guidance.

## Verification

See `verification.txt` for final checks. Acquisition lifecycle uses synthetic fixes in a
JVM harness; Android adapter APK is compiled, signed and verified against the installed SDK.
No production tracking, ingest, settings, installations, email or archive operations were
performed. Read-only phone inspection confirmed the existing helper/uploader pattern.

## Deployment

Follow [the rollout and rollback runbook](../../docs/android-location-rollout.md).
Server code and warehouse provisioning must precede phone activation. Installation starts
paused, reuses the existing PDW account/configuration and grants coarse/background only.
Rollback pauses and stops this collector and revokes its permissions while retaining queued
samples and warehouse history.

Physical-device acquisition, Doze cadence, accuracy and battery drain are unmeasured.
The runbook specifies the comparable-day trial and acceptance targets; tests do not imply
fifteen-minute hardware reliability or a measured battery claim.
