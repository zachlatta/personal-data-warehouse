"""Real one-shot acquisition logic, run against a fake source, never a phone.

The Android SDK adapter is also compiled by the helper APK build. These tests
prove the acquisition lifecycle and durable queue on a JVM; they do not claim
Android permission, Doze cadence, network-provider availability or battery tests.
"""

from pathlib import Path
import shutil
import subprocess
import xml.etree.ElementTree as ET

import pytest

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "ops/android/pdw-location-helper"


def test_fake_location_acquisition_lifecycle_and_queue(tmp_path):
    if not shutil.which("javac") or not shutil.which("java"):
        pytest.fail("JDK required to validate the Android location acquisition core")
    harness = tmp_path / "CaptureTest.java"
    harness.write_text('''
import com.zachlatta.pdw.location.LocationCapture;
import java.io.*;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicInteger;
public class CaptureTest {
  static final long MAX_CACHED_AGE_MS = 15 * 60 * 1000L;
  static long now = 500_000_000_000L;
  static LocationCapture.Fix fix(long elapsed) {
    return new LocationCapture.Fix(1.25, -2.5, 250, 1700000000000L, elapsed);
  }
  static void check(boolean value) { if (!value) throw new AssertionError(); }
  static void fails(LocationCapture.Source source, File dir, File stateFile, String expectedCode) throws Exception {
    try {
      LocationCapture.captureWithCached(() -> null, source, dir, stateFile, now, MAX_CACHED_AGE_MS, 5);
      throw new AssertionError("accepted invalid fix");
    } catch (LocationCapture.CaptureException expected) {
      if (!expectedCode.equals(expected.code)) {
        throw new AssertionError("expected code " + expectedCode + ", got " + expected.code);
      }
    }
  }
  public static void main(String[] args) throws Exception {
    File dir = new File(args[0], "spool");
    File stateFile = new File(args[0], "last-elapsed-nanos");
    AtomicInteger requested = new AtomicInteger(), cancelled = new AtomicInteger();
    LocationCapture.Source source = callback -> {
      int n = requested.incrementAndGet();
      callback.accept(fix(now + n));
      return () -> cancelled.incrementAndGet();
    };
    File first = LocationCapture.captureWithCached(() -> null, source, dir, stateFile, now, MAX_CACHED_AGE_MS, 5);
    check(requested.get() == 1 && cancelled.get() == 1);
    String json = Files.readString(first.toPath());
    check(json.contains("\\\"measured_at_ms\\\":1700000000000"));
    check(json.contains("\\\"accuracy_m\\\":250.000"));
    check(json.contains("\\\"latitude\\\":1.250000000"));
    check(json.contains("\\\"provider\\\":\\\"network\\\""));
    check(json.contains("\\\"sample_id\\\":"));
    // Another fix never overwrites a queued offline sample, even same measurement time.
    File second = LocationCapture.captureWithCached(() -> null, source, dir, stateFile, now, MAX_CACHED_AGE_MS, 5);
    check(!first.equals(second) && first.exists() && second.exists());
    check(new File(dir, ".tmp").list().length == 0);
    // An invalid cached fix (e.g. NaN latitude) is ignored and falls through to the active source.
    File third = LocationCapture.captureWithCached(
        () -> new LocationCapture.Fix(Double.NaN, -2.5, 250, 1700000000000L, now + 10),
        source, dir, stateFile, now, MAX_CACHED_AGE_MS, 5);
    check(requested.get() == 3 && third.exists());
    AtomicInteger timeoutCancel = new AtomicInteger();
    fails(callback -> () -> timeoutCancel.incrementAndGet(), dir, stateFile, "timeout");
    check(timeoutCancel.get() == 1);
    fails(callback -> { callback.accept(null); return () -> {}; }, dir, stateFile, "no_fresh_fix");
    File freshStateFile = new File(args[0], "stale-last-elapsed-nanos");
    fails(callback -> { callback.accept(fix(now - 121_000_000_000L)); return () -> {}; }, dir, freshStateFile, "no_fresh_fix");
    fails(callback -> { callback.accept(fix(now + 1_000_000_000L)); return () -> {}; }, dir, stateFile, "no_fresh_fix");
    fails(callback -> { callback.accept(new LocationCapture.Fix(Double.NaN, 0, 2, 1, now + 20)); return () -> {}; }, dir, stateFile, "no_fresh_fix");
    fails(callback -> { callback.accept(new LocationCapture.Fix(0, 0, 0, 1, now + 20)); return () -> {}; }, dir, stateFile, "no_fresh_fix");
    check(dir.list((d, name) -> name.endsWith(".json")).length == 3);
    // Permission/provider errors fail closed and keep the existing offline queue.
    try {
      LocationCapture.captureWithCached(() -> null, callback -> { throw new SecurityException("denied"); },
          dir, stateFile, now, MAX_CACHED_AGE_MS, 5);
      throw new AssertionError();
    } catch (SecurityException expected) { }
    check(first.exists() && second.exists() && third.exists());
    // Interrupted acquisition must cancel too.
    Thread.currentThread().interrupt();
    try {
      LocationCapture.captureWithCached(() -> null, callback -> () -> cancelled.incrementAndGet(),
          dir, stateFile, now, MAX_CACHED_AGE_MS, 5);
      throw new AssertionError();
    } catch (InterruptedException expected) { }
    check(cancelled.get() == 4);
    System.out.println("PASS: fake fresh/null/stale/future/invalid/timeout/permission/interrupted acquisition; offline queue survives");
  }
}
''')
    source = HELPER / "src/com/zachlatta/pdw/location/LocationCapture.java"
    subprocess.run(["javac", "--release", "11", "-d", str(tmp_path), str(source), str(harness)], check=True)
    result = subprocess.run(["java", "-cp", str(tmp_path), "CaptureTest", str(tmp_path)], check=True, capture_output=True, text=True)
    assert "PASS: fake fresh" in result.stdout


def test_location_helper_permissions_are_coarse_and_shell_only():
    manifest = ET.parse(HELPER / "AndroidManifest.xml").getroot()
    android = "{http://schemas.android.com/apk/res/android}"
    permissions = {p.attrib[android + "name"] for p in manifest.findall("uses-permission")}
    assert permissions == {"android.permission.ACCESS_COARSE_LOCATION", "android.permission.ACCESS_BACKGROUND_LOCATION"}
    provider = manifest.find("application/provider")
    assert provider.attrib[android + "permission"] == "android.permission.DUMP"
    adapter = (HELPER / "src/com/zachlatta/pdw/location/LocationProvider.java").read_text()
    assert "getCurrentLocation(LocationManager.NETWORK_PROVIDER" in adapter
    assert "getLastKnownLocation(LocationManager.NETWORK_PROVIDER)" in adapter
    assert "captureWithCached(" in adapter
    assert "cancellation::cancel" in adapter
    assert "error_code" in adapter
    assert "permission_denied" in adapter
    assert "location_disabled" in adapter
    assert "isLocationEnabled()" in adapter
    assert "isProviderEnabled(LocationManager.NETWORK_PROVIDER)" in adapter
    capture_src = (HELPER / "src/com/zachlatta/pdw/location/LocationCapture.java").read_text()
    assert "public static File capture(" not in capture_src


def test_bounded_deduplicated_cached_network_fix_avoids_active_request(tmp_path):
    if not shutil.which("javac") or not shutil.which("java"):
        pytest.fail("JDK required to validate the Android location acquisition core")
    harness = tmp_path / "CachedCaptureTest.java"
    harness.write_text('''
import com.zachlatta.pdw.location.LocationCapture;
import java.io.*;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicInteger;
public class CachedCaptureTest {
  static final long MAX_CACHED_AGE_MS = 15 * 60 * 1000L;
  static LocationCapture.Fix fix(long measuredAtMs, long elapsed) {
    return new LocationCapture.Fix(1.25, -2.5, 2000, measuredAtMs, elapsed);
  }
  static void check(boolean value, String msg) { if (!value) throw new AssertionError(msg); }
  public static void main(String[] args) throws Exception {
    File dir = new File(args[0], "spool");
    File stateFile = new File(args[0], "last-elapsed-nanos");
    long now = 2_000_000_000_000L; // 2000s uptime
    long fiveMinAgo = now - 300_000_000_000L;
    AtomicInteger activeCalls = new AtomicInteger();
    LocationCapture.Source timeoutActive = cb -> {
      activeCalls.incrementAndGet();
      return () -> {};
    };

    // 1. Fresh cached network fix (5 min old <= 15 min) succeeds immediately without active call.
    File f1 = LocationCapture.captureWithCached(
        () -> fix(1700000000000L, fiveMinAgo), timeoutActive, dir, stateFile,
        now, MAX_CACHED_AGE_MS, 5);
    check(activeCalls.get() == 0, "should not invoke active source when fresh cached fix exists");
    check(Files.readString(f1.toPath()).contains("\\"measured_at_ms\\":1700000000000"), "original measured_at_ms preserved");

    // 2. Repeating the exact same cached fix is deduplicated and falls back to active source.
    try {
      LocationCapture.captureWithCached(
          () -> fix(1700000000000L, fiveMinAgo), timeoutActive, dir, stateFile,
          now + 60_000_000_000L, MAX_CACHED_AGE_MS, 5);
      throw new AssertionError("duplicate cached fix should not be accepted");
    } catch (LocationCapture.CaptureException expected) {
      check("timeout".equals(expected.code), "expected timeout fallback, got " + expected.code);
    }
    check(activeCalls.get() == 1, "duplicate cached fix must fall back to active source");

    // 3. Stale cached fix (> 15 min old) is rejected and falls back to active source.
    long sixteenMinAgo = (now + 120_000_000_000L) - 960_000_000_000L;
    try {
      LocationCapture.captureWithCached(
          () -> fix(1700000001000L, sixteenMinAgo), timeoutActive, dir, stateFile,
          now + 120_000_000_000L, MAX_CACHED_AGE_MS, 5);
      throw new AssertionError("stale cached fix should not be accepted");
    } catch (LocationCapture.CaptureException expected) {
      check("timeout".equals(expected.code), "expected timeout fallback on stale cache, got " + expected.code);
    }
    check(activeCalls.get() == 2, "stale cached fix must fall back to active source");

    // 4. Newer cached fix (e.g. 10 min later) is accepted without active call.
    long laterNow = now + 900_000_000_000L;
    long laterFix = laterNow - 180_000_000_000L;
    File f2 = LocationCapture.captureWithCached(
        () -> fix(1700000900000L, laterFix), timeoutActive, dir, stateFile,
        laterNow, MAX_CACHED_AGE_MS, 5);
    check(activeCalls.get() == 2, "newer cached fix should not invoke active source");
    check(!f1.equals(f2) && f2.exists(), "newer cached fix committed");

    // 5. After a reboot (monotonic clock reset: now < persisted last-elapsed-nanos), fresh cached fix is accepted.
    long postRebootNow = 500_000_000_000L;
    File f3 = LocationCapture.captureWithCached(
        () -> fix(1700005000000L, postRebootNow - 60_000_000_000L), timeoutActive, dir, stateFile,
        postRebootNow, MAX_CACHED_AGE_MS, 5);
    check(activeCalls.get() == 2, "post-reboot fresh cached fix should not invoke active source");
    check(f3.exists(), "post-reboot fix committed");

    // 6. Active fallback returning the same already-emitted fix is rejected as no_fresh_fix.
    LocationCapture.Source sameActive = cb -> {
      activeCalls.incrementAndGet();
      cb.accept(fix(1700005000000L, postRebootNow - 60_000_000_000L));
      return () -> {};
    };
    try {
      LocationCapture.captureWithCached(
          () -> null, sameActive, dir, stateFile, postRebootNow + 10_000_000_000L, MAX_CACHED_AGE_MS, 5);
      throw new AssertionError("duplicate active fix should be rejected");
    } catch (LocationCapture.CaptureException expected) {
      check("no_fresh_fix".equals(expected.code), "expected no_fresh_fix, got " + expected.code);
    }
    System.out.println("PASS: bounded deduplicated cached network fix");
  }
}
''')
    source = HELPER / "src/com/zachlatta/pdw/location/LocationCapture.java"
    subprocess.run(["javac", "--release", "11", "-d", str(tmp_path), str(source), str(harness)], check=True)
    result = subprocess.run(["java", "-cp", str(tmp_path), "CachedCaptureTest", str(tmp_path)], check=True, capture_output=True, text=True)
    assert "PASS: bounded deduplicated cached network fix" in result.stdout
