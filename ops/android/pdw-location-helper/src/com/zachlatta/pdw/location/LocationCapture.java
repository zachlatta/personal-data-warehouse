package com.zachlatta.pdw.location;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
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
    public interface DirectorySync { void sync(File directory) throws IOException; }
    public static final class Fix {
        public final double latitude, longitude, accuracy;
        public final long measuredAtMs, elapsedNanos;
        public Fix(double lat, double lon, double accuracy, long measuredAtMs, long elapsedNanos) {
            this.latitude = lat; this.longitude = lon; this.accuracy = accuracy;
            this.measuredAtMs = measuredAtMs; this.elapsedNanos = elapsedNanos;
        }
    }
    public static File capture(Source source, File spool, long nowElapsedNanos, long timeoutMs)
            throws IOException, InterruptedException {
        return capture(source, spool, nowElapsedNanos, timeoutMs, directory -> {
            try (FileChannel channel = FileChannel.open(directory.toPath(), StandardOpenOption.READ)) {
                channel.force(true);
            }
        });
    }
    public static File capture(Source source, File spool, long nowElapsedNanos, long timeoutMs, DirectorySync sync)
            throws IOException, InterruptedException {
        CountDownLatch ready = new CountDownLatch(1);
        AtomicReference<Fix> result = new AtomicReference<>();
        Runnable cancel = source.request(fix -> { result.set(fix); ready.countDown(); });
        try {
            if (!ready.await(timeoutMs, TimeUnit.MILLISECONDS)) throw new IOException("network location timeout");
        } finally {
            cancel.run();
        }
        Fix f = result.get();
        // A cached network fix can be returned. Keep its original measurement time;
        // reject old/future fixes rather than relabelling an old position as current.
        if (f == null || f.elapsedNanos <= 0 || f.elapsedNanos < nowElapsedNanos - 120_000_000_000L
                || f.elapsedNanos > nowElapsedNanos + timeoutMs * 1_000_000L
                || f.measuredAtMs <= 0 || !Double.isFinite(f.latitude) || Math.abs(f.latitude) > 90
                || !Double.isFinite(f.longitude) || Math.abs(f.longitude) > 180
                || !Double.isFinite(f.accuracy) || f.accuracy <= 0) {
            throw new IOException("no fresh valid network location");
        }
        String id = UUID.randomUUID().toString();
        String json = String.format(Locale.ROOT,
                "{\"record_type\":\"location\",\"sample_id\":\"%s\",\"latitude\":%.9f,\"longitude\":%.9f,"
                + "\"accuracy_m\":%.3f,\"measured_at_ms\":%d,\"helper_received_at_ms\":%d,\"provider\":\"network\"}\n",
                id, f.latitude, f.longitude, f.accuracy, f.measuredAtMs, System.currentTimeMillis());
        File tmp = new File(spool, ".tmp");
        if (!tmp.isDirectory() && !tmp.mkdirs()) throw new IOException("cannot create private spool");
        String name = f.measuredAtMs + "-" + id + ".json";
        File partial = new File(tmp, name), complete = new File(spool, name);
        try (FileOutputStream out = new FileOutputStream(partial)) {
            out.write(json.getBytes(StandardCharsets.UTF_8));
            out.getFD().sync();
        }
        if (!partial.renameTo(complete)) throw new IOException("cannot commit location sample");
        // Persist the renamed directory entry too, not just the file contents.
        sync.sync(spool);
        // No deletion/retention here: only the authenticated uploader's ack removes a sample.
        return complete;
    }
}
