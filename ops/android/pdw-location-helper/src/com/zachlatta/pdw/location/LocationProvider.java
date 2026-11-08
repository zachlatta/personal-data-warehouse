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
                try {
                    ParcelFileDescriptor.AutoCloseOutputStream out =
                            new ParcelFileDescriptor.AutoCloseOutputStream(pipe[1]);
                    File sample = capture();
                    out.write(("{\"queued\":\"" + sample.getName() + "\"}\n").getBytes(StandardCharsets.UTF_8));
                    out.close();
                } catch (Exception e) {
                    try { pipe[1].closeWithError("location acquisition failed: " + e); } catch (Exception ignored) { }
                }
            }, "pdw-network-location").start();
            return pipe[0];
        } catch (Exception e) { throw new FileNotFoundException(e.toString()); }
    }
    private File capture() throws Exception {
        if (!busy.compareAndSet(false, true)) throw new java.io.IOException("acquisition already in progress");
        try {
            LocationManager manager = (LocationManager) getContext().getSystemService(Context.LOCATION_SERVICE);
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
