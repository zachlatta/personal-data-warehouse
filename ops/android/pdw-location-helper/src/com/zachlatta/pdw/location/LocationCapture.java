package com.zachlatta.pdw.location;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Platform-free one-shot acquisition and durable handoff, exercised with fake fixes. */
public final class LocationCapture {
    public interface Callback { void accept(Fix fix); }
    public interface Source { Runnable request(Callback callback); }
    public interface CachedSource { Fix get(); }
    public interface DirectorySync { void sync(File directory) throws IOException; }
    public static final class CaptureException extends IOException {
        public final String code;
        public CaptureException(String code, String message) {
            super(message);
            this.code = code;
        }
    }
    public static final class Fix {
        public final double latitude, longitude, accuracy;
        public final long measuredAtMs, elapsedNanos;
        public Fix(double lat, double lon, double accuracy, long measuredAtMs, long elapsedNanos) {
            this.latitude = lat; this.longitude = lon; this.accuracy = accuracy;
            this.measuredAtMs = measuredAtMs; this.elapsedNanos = elapsedNanos;
        }
    }
    /**
     * Prefers a bounded, un-emitted cached network fix before issuing an active
     * getCurrentLocation() call. AOSP LocationProviderManager clamps COARSE-only
     * active requests to MIN_COARSE_INTERVAL_MS (10 min), so a 30 s active call
     * frequently times out even when the network provider already holds a fresh
     * fix from minutes ago. Preserves the original measurement time and deduplicates
     * by elapsedRealtimeNanos so stale or repeated fixes are never emitted.
     */
    public static File captureWithCached(CachedSource cached, Source active, File spool, File lastElapsedFile,
            long nowElapsedNanos, long maxCachedAgeMs, long timeoutMs)
            throws IOException, InterruptedException {
        return captureWithCached(cached, active, spool, lastElapsedFile,
                nowElapsedNanos, maxCachedAgeMs, timeoutMs, LocationCapture::defaultSync);
    }
    public static File captureWithCached(CachedSource cached, Source active, File spool, File lastElapsedFile,
            long nowElapsedNanos, long maxCachedAgeMs, long timeoutMs, DirectorySync sync)
            throws IOException, InterruptedException {
        long lastEmitted = readLastElapsedNanos(lastElapsedFile, nowElapsedNanos, timeoutMs);
        long maxCachedAgeNanos = maxCachedAgeMs * 1_000_000L;
        long futureSlackNanos = timeoutMs * 1_000_000L;
        if (cached != null) {
            Fix cachedFix = cached.get();
            if (isValidFix(cachedFix, nowElapsedNanos, maxCachedAgeNanos, futureSlackNanos)
                    && cachedFix.elapsedNanos > lastEmitted) {
                File committed = commitFix(cachedFix, spool, sync);
                writeLastElapsedNanos(lastElapsedFile, cachedFix.elapsedNanos);
                return committed;
            }
        }
        Fix activeFix = acquireActive(active, timeoutMs);
        if (!isValidFix(activeFix, nowElapsedNanos, 120_000_000_000L, futureSlackNanos)
                || activeFix.elapsedNanos <= lastEmitted) {
            throw new CaptureException("no_fresh_fix", "no fresh valid network location");
        }
        File committed = commitFix(activeFix, spool, sync);
        writeLastElapsedNanos(lastElapsedFile, activeFix.elapsedNanos);
        return committed;
    }
    private static void defaultSync(File directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory.toPath(), StandardOpenOption.READ)) {
            channel.force(true);
        }
    }
    private static Fix acquireActive(Source source, long timeoutMs) throws IOException, InterruptedException {
        CountDownLatch ready = new CountDownLatch(1);
        AtomicReference<Fix> result = new AtomicReference<>();
        Runnable cancel = source.request(fix -> { result.set(fix); ready.countDown(); });
        try {
            if (!ready.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                throw new CaptureException("timeout", "network location timeout");
            }
        } finally {
            cancel.run();
        }
        return result.get();
    }
    private static boolean isValidFix(Fix f, long nowElapsedNanos, long maxAgeNanos, long futureSlackNanos) {
        return f != null && f.elapsedNanos > 0
                && f.elapsedNanos >= nowElapsedNanos - maxAgeNanos
                && f.elapsedNanos <= nowElapsedNanos + futureSlackNanos
                && f.measuredAtMs > 0
                && Double.isFinite(f.latitude) && Math.abs(f.latitude) <= 90
                && Double.isFinite(f.longitude) && Math.abs(f.longitude) <= 180
                && Double.isFinite(f.accuracy) && f.accuracy > 0;
    }
    private static long readLastElapsedNanos(File stateFile, long nowElapsedNanos, long timeoutMs) {
        if (stateFile == null || !stateFile.isFile()) return 0;
        try {
            long stored = Long.parseLong(Files.readString(stateFile.toPath(), StandardCharsets.UTF_8).trim());
            // A reboot resets SystemClock.elapsedRealtimeNanos(), so a stored value ahead
            // of the current monotonic clock belongs to a previous boot.
            if (stored <= 0 || stored > nowElapsedNanos + timeoutMs * 1_000_000L) return 0;
            return stored;
        } catch (Exception ignored) {
            return 0;
        }
    }
    private static void writeLastElapsedNanos(File stateFile, long elapsedNanos) {
        if (stateFile == null) return;
        try {
            File parent = stateFile.getParentFile();
            if (parent != null && !parent.isDirectory()) parent.mkdirs();
            Files.writeString(stateFile.toPath(), Long.toString(elapsedNanos) + "\n", StandardCharsets.UTF_8);
        } catch (Exception ignored) { }
    }
    private static File commitFix(Fix f, File spool, DirectorySync sync) throws IOException {
        String id = UUID.randomUUID().toString();
        String json = String.format(Locale.ROOT,
                "{\"record_type\":\"location\",\"sample_id\":\"%s\",\"latitude\":%.9f,\"longitude\":%.9f,"
                + "\"accuracy_m\":%.3f,\"measured_at_ms\":%d,\"helper_received_at_ms\":%d,\"provider\":\"network\"}\n",
                id, f.latitude, f.longitude, f.accuracy, f.measuredAtMs, System.currentTimeMillis());
        File tmp = new File(spool, ".tmp");
        if (!tmp.isDirectory() && !tmp.mkdirs()) {
            throw new CaptureException("storage_error", "cannot create private spool");
        }
        String name = f.measuredAtMs + "-" + id + ".json";
        File partial = new File(tmp, name), complete = new File(spool, name);
        try (FileOutputStream out = new FileOutputStream(partial)) {
            out.write(json.getBytes(StandardCharsets.UTF_8));
            out.getFD().sync();
        }
        if (!partial.renameTo(complete)) {
            throw new CaptureException("storage_error", "cannot commit location sample");
        }
        // Persist the renamed directory entry too, not just the file contents.
        sync.sync(spool);
        // No deletion/retention here: only the authenticated uploader's ack removes a sample.
        return complete;
    }
}
