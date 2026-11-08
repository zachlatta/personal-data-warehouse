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
  static long now = 500_000_000_000L;
  static LocationCapture.Fix fix(long elapsed) {
    return new LocationCapture.Fix(1.25, -2.5, 250, 1700000000000L, elapsed);
  }
  static void check(boolean value) { if (!value) throw new AssertionError(); }
  static void fails(LocationCapture.Source source, File dir) throws Exception {
    try { LocationCapture.capture(source, dir, now, 5); throw new AssertionError("accepted invalid fix"); }
    catch (IOException expected) { }
  }
  public static void main(String[] args) throws Exception {
    File dir = new File(args[0], "spool");
    AtomicInteger requested = new AtomicInteger(), cancelled = new AtomicInteger();
    LocationCapture.Source source = callback -> {
      requested.incrementAndGet(); callback.accept(fix(now)); return () -> cancelled.incrementAndGet();
    };
    File first = LocationCapture.capture(source, dir, now, 5);
    check(requested.get() == 1 && cancelled.get() == 1);
    String json = Files.readString(first.toPath());
    check(json.contains("\\\"measured_at_ms\\\":1700000000000"));
    check(json.contains("\\\"accuracy_m\\\":250.000"));
    check(json.contains("\\\"latitude\\\":1.250000000"));
    check(json.contains("\\\"provider\\\":\\\"network\\\""));
    check(json.contains("\\\"sample_id\\\":"));
    // Another fix never overwrites a queued offline sample, even same measurement time.
    File second = LocationCapture.capture(source, dir, now, 5);
    check(!first.equals(second) && first.exists() && second.exists());
    check(new File(dir, ".tmp").list().length == 0);
    AtomicInteger timeoutCancel = new AtomicInteger();
    fails(callback -> () -> timeoutCancel.incrementAndGet(), dir);
    check(timeoutCancel.get() == 1);
    fails(callback -> { callback.accept(null); return () -> {}; }, dir);
    fails(callback -> { callback.accept(fix(now - 121_000_000_000L)); return () -> {}; }, dir);
    fails(callback -> { callback.accept(fix(now + 1_000_000_000L)); return () -> {}; }, dir);
    fails(callback -> { callback.accept(new LocationCapture.Fix(Double.NaN, 0, 2, 1, now)); return () -> {}; }, dir);
    fails(callback -> { callback.accept(new LocationCapture.Fix(0, 0, 0, 1, now)); return () -> {}; }, dir);
    check(dir.list((d, name) -> name.endsWith(".json")).length == 2);
    // Permission/provider errors fail closed and keep the existing offline queue.
    try { LocationCapture.capture(callback -> { throw new SecurityException("denied"); }, dir, now, 5);
      throw new AssertionError(); } catch (SecurityException expected) { }
    check(first.exists() && second.exists());
    // Interrupted acquisition must cancel too.
    Thread.currentThread().interrupt();
    try { LocationCapture.capture(callback -> () -> cancelled.incrementAndGet(), dir, now, 5);
      throw new AssertionError(); } catch (InterruptedException expected) { }
    check(cancelled.get() == 3);
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
    assert "cancellation::cancel" in adapter
