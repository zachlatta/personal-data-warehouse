package com.zachlatta.pdw.location;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.location.LocationManager;
import android.net.Uri;
import android.os.CancellationSignal;
import android.os.ParcelFileDescriptor;
import android.os.SystemClock;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;

/** No network permission, no GPS, no always-running service. Root owns cadence and upload. */
public final class LocationProvider extends ContentProvider {
    private final java.util.concurrent.atomic.AtomicBoolean busy = new java.util.concurrent.atomic.AtomicBoolean();
    @Override public boolean onCreate() { return true; }
    @Override public ParcelFileDescriptor openFile(Uri uri, String mode) throws FileNotFoundException {
        if (!"/sample".equals(uri.getPath()) || !"r".equals(mode))
            throw new FileNotFoundException("use read-only /sample");
        try {
            ParcelFileDescriptor[] pipe = ParcelFileDescriptor.createReliablePipe();
            new Thread(() -> {
                try (ParcelFileDescriptor.AutoCloseOutputStream out =
                        new ParcelFileDescriptor.AutoCloseOutputStream(pipe[1])) {
                    try {
                        File sample = capture();
                        out.write(("{\"queued\":\"" + sample.getName() + "\"}\n").getBytes(StandardCharsets.UTF_8));
                    } catch (SecurityException e) {
                        writeError(out, "permission_denied", "location permission denied");
                    } catch (LocationCapture.CaptureException e) {
                        writeError(out, e.code, e.getMessage());
                    } catch (Exception e) {
                        writeError(out, "error", e.getClass().getSimpleName());
                    }
                } catch (Exception ignored) { }
            }, "pdw-network-location").start();
            return pipe[0];
        } catch (Exception e) { throw new FileNotFoundException(e.toString()); }
    }
    private static void writeError(ParcelFileDescriptor.AutoCloseOutputStream out, String code, String message)
            throws java.io.IOException {
        String safeCode = code == null || code.isEmpty() ? "error" : code.replace("\"", "");
        String safeMsg = message == null || message.isEmpty() ? safeCode : message.replace("\\", "\\\\").replace("\"", "'").replace("\n", " ");
        out.write(("{\"error_code\":\"" + safeCode + "\",\"error\":\"" + safeMsg + "\"}\n").getBytes(StandardCharsets.UTF_8));
    }
    private File capture() throws Exception {
        if (!busy.compareAndSet(false, true)) {
            throw new LocationCapture.CaptureException("busy", "acquisition already in progress");
        }
        try {
            LocationManager manager = (LocationManager) getContext().getSystemService(Context.LOCATION_SERVICE);
            if (manager == null || !manager.isLocationEnabled()
                    || !manager.isProviderEnabled(LocationManager.NETWORK_PROVIDER)) {
                throw new LocationCapture.CaptureException("location_disabled", "network location provider disabled");
            }
            return LocationCapture.capture(callback -> {
                CancellationSignal cancellation = new CancellationSignal();
                // Provider selection is explicit. There is deliberately no fallback to GPS,
                // fused high accuracy, requestLocationUpdates, or last-known-location polling.
                manager.getCurrentLocation(LocationManager.NETWORK_PROVIDER, cancellation, Runnable::run,
                        location -> callback.accept(location == null ? null : new LocationCapture.Fix(
                                location.getLatitude(), location.getLongitude(), location.getAccuracy(),
                                location.getTime(), location.getElapsedRealtimeNanos())));
                return cancellation::cancel;
            }, new File(getContext().getFilesDir(), "spool"), SystemClock.elapsedRealtimeNanos(), 30_000, directory -> {
                try {
                    java.io.FileDescriptor fd = android.system.Os.open(directory.getPath(),
                            android.system.OsConstants.O_RDONLY, 0);
                    try { android.system.Os.fsync(fd); } finally { android.system.Os.close(fd); }
                } catch (android.system.ErrnoException e) { throw new java.io.IOException(e); }
            });
        } finally { busy.set(false); }
    }
    @Override public String getType(Uri uri) { return "application/json"; }
    @Override public Cursor query(Uri u, String[] p, String s, String[] a, String o) { throw new UnsupportedOperationException(); }
    @Override public Uri insert(Uri u, ContentValues v) { throw new UnsupportedOperationException(); }
    @Override public int delete(Uri u, String s, String[] a) { throw new UnsupportedOperationException(); }
    @Override public int update(Uri u, ContentValues v, String s, String[] a) { throw new UnsupportedOperationException(); }
}
