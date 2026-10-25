package com.zachlatta.pdw.usage;

import android.app.usage.EventStats;
import android.app.usage.UsageEvents;
import android.app.usage.UsageStats;
import android.app.usage.UsageStatsManager;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.pm.ApplicationInfo;
import android.content.pm.PackageManager;
import android.database.Cursor;
import android.net.Uri;
import android.os.Build;
import android.os.ParcelFileDescriptor;
import android.util.Log;

import org.json.JSONObject;

import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Streams what Android records about how the phone is used, as JSON lines,
 * to `content read` (run as root by `pdw ingest android-screen-time`):
 *
 *   content://com.zachlatta.pdw.usage/events?begin=MS&end=MS
 *     every UsageEvents event in [begin, end): an app coming to the
 *     foreground or leaving it, the screen turning on or off, the phone
 *     locking or unlocking, a notification interrupting, shutdown, startup.
 *     Android keeps these for about ten days.
 *   content://com.zachlatta.pdw.usage/daily?begin=MS&end=MS
 *     Android's own daily totals in that range: per app (time in the
 *     foreground, time visible, launches) and for the device (time with the
 *     screen on, time unlocked). Android keeps daily buckets for about ten
 *     days, then rolls them into weekly, monthly and yearly ones.
 *
 * The app reads these through the public UsageStatsManager API, so it needs
 * usage access, which the installer grants with
 * `appops set com.zachlatta.pdw.usage GET_USAGE_STATS allow`. The provider is
 * guarded by android.permission.DUMP, which only the shell and root hold.
 * Everything is computed on the request; the app keeps no data and has no
 * network access.
 */
public class UsageProvider extends ContentProvider {
    private static final String TAG = "PdwUsage";
    static final int EXPORTER_VERSION = 1;

    @Override
    public boolean onCreate() {
        return true;
    }

    @Override
    public ParcelFileDescriptor openFile(Uri uri, String mode) throws FileNotFoundException {
        final String path = uri.getPath() == null ? "" : uri.getPath();
        final long begin = longParam(uri, "begin", 0);
        final long end = longParam(uri, "end", System.currentTimeMillis());
        if (!path.equals("/events") && !path.equals("/daily")) {
            throw new FileNotFoundException("unknown path " + path + " (use /events or /daily)");
        }
        final ParcelFileDescriptor[] pipe;
        try {
            pipe = ParcelFileDescriptor.createReliablePipe();
        } catch (java.io.IOException e) {
            throw new FileNotFoundException("pipe: " + e);
        }
        final ParcelFileDescriptor writeSide = pipe[1];
        new Thread(() -> {
            OutputStream raw = new ParcelFileDescriptor.AutoCloseOutputStream(writeSide);
            try (Writer out = new OutputStreamWriter(raw, StandardCharsets.UTF_8)) {
                if (path.equals("/events")) {
                    writeEvents(out, begin, end);
                } else {
                    writeDaily(out, begin, end);
                }
                out.flush();
            } catch (Exception e) {
                Log.w(TAG, "export failed", e);
                try {
                    writeSide.closeWithError(String.valueOf(e));
                } catch (java.io.IOException ignored) {
                }
            }
        }, "pdw-usage-export").start();
        return pipe[0];
    }

    private void writeEvents(Writer out, long begin, long end) throws Exception {
        UsageStatsManager usm = (UsageStatsManager) getContext().getSystemService(Context.USAGE_STATS_SERVICE);
        UsageEvents events = usm.queryEvents(begin, end);
        if (events == null) {
            throw new IllegalStateException("queryEvents returned null (no usage access?)");
        }
        UsageEvents.Event e = new UsageEvents.Event();
        while (events.hasNextEvent()) {
            if (!events.getNextEvent(e)) {
                break;
            }
            JSONObject o = new JSONObject();
            o.put("record_type", "usage_event");
            o.put("event_at_ms", e.getTimeStamp());
            o.put("event_type", e.getEventType());
            o.put("event_type_name", eventTypeName(e.getEventType()));
            o.put("package_name", nz(e.getPackageName()));
            o.put("class_name", nz(e.getClassName()));
            if (e.getEventType() == UsageEvents.Event.SHORTCUT_INVOCATION) {
                o.put("shortcut_id", nz(e.getShortcutId()));
            }
            if (e.getEventType() == UsageEvents.Event.STANDBY_BUCKET_CHANGED) {
                o.put("standby_bucket", e.getAppStandbyBucket());
            }
            if (e.getEventType() == UsageEvents.Event.CONFIGURATION_CHANGE && e.getConfiguration() != null) {
                o.put("configuration", e.getConfiguration().toString());
            }
            // Not in the public API; read when the platform allows it. The
            // instance id pairs an activity's resume with its pause.
            putHidden(o, e, "mInstanceId", "instance_id");
            putHidden(o, e, "mTaskRootPackage", "task_root_package");
            putHidden(o, e, "mTaskRootClass", "task_root_class");
            putHidden(o, e, "mNotificationChannelId", "notification_channel_id");
            o.put("exporter_version", EXPORTER_VERSION);
            out.write(o.toString());
            out.write('\n');
        }
    }

    private void writeDaily(Writer out, long begin, long end) throws Exception {
        UsageStatsManager usm = (UsageStatsManager) getContext().getSystemService(Context.USAGE_STATS_SERVICE);
        String tz = TimeZone.getDefault().getID();
        Map<String, String> labels = new HashMap<>();
        List<UsageStats> apps = usm.queryUsageStats(UsageStatsManager.INTERVAL_DAILY, begin, end);
        if (apps != null) {
            for (UsageStats s : apps) {
                JSONObject o = new JSONObject();
                o.put("record_type", "daily_app_usage");
                o.put("package_name", nz(s.getPackageName()));
                o.put("app_label", label(labels, s.getPackageName()));
                o.put("bucket_start_ms", s.getFirstTimeStamp());
                o.put("bucket_end_ms", s.getLastTimeStamp());
                o.put("total_time_in_foreground_ms", s.getTotalTimeInForeground());
                o.put("last_time_used_ms", s.getLastTimeUsed());
                if (Build.VERSION.SDK_INT >= 29) {
                    o.put("total_time_visible_ms", s.getTotalTimeVisible());
                    o.put("last_time_visible_ms", s.getLastTimeVisible());
                    o.put("total_time_foreground_service_ms", s.getTotalTimeForegroundServiceUsed());
                    o.put("last_time_foreground_service_ms", s.getLastTimeForegroundServiceUsed());
                }
                putHidden(o, s, "mAppLaunchCount", "app_launch_count");
                o.put("time_zone", tz);
                o.put("exporter_version", EXPORTER_VERSION);
                out.write(o.toString());
                out.write('\n');
            }
        }
        List<EventStats> device = usm.queryEventStats(UsageStatsManager.INTERVAL_DAILY, begin, end);
        if (device != null) {
            for (EventStats s : device) {
                JSONObject o = new JSONObject();
                o.put("record_type", "daily_device_usage");
                o.put("event_type", s.getEventType());
                o.put("event_type_name", eventTypeName(s.getEventType()));
                o.put("bucket_start_ms", s.getFirstTimeStamp());
                o.put("bucket_end_ms", s.getLastTimeStamp());
                o.put("count", s.getCount());
                o.put("total_time_ms", s.getTotalTime());
                o.put("last_event_ms", s.getLastEventTime());
                o.put("time_zone", tz);
                o.put("exporter_version", EXPORTER_VERSION);
                out.write(o.toString());
                out.write('\n');
            }
        }
    }

    private String label(Map<String, String> cache, String pkg) {
        if (pkg == null) {
            return "";
        }
        String cached = cache.get(pkg);
        if (cached != null) {
            return cached;
        }
        String label = "";
        try {
            PackageManager pm = getContext().getPackageManager();
            ApplicationInfo info = pm.getApplicationInfo(pkg, 0);
            CharSequence l = pm.getApplicationLabel(info);
            label = l == null ? "" : l.toString();
        } catch (Exception ignored) {
        }
        cache.put(pkg, label);
        return label;
    }

    private static void putHidden(JSONObject o, Object source, String field, String key) {
        try {
            Field f = source.getClass().getDeclaredField(field);
            f.setAccessible(true);
            Object v = f.get(source);
            if (v != null) {
                o.put(key, v);
            }
        } catch (Throwable ignored) {
        }
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }

    private static long longParam(Uri uri, String name, long fallback) {
        String v = uri.getQueryParameter(name);
        if (v == null || v.isEmpty()) {
            return fallback;
        }
        return Long.parseLong(v);
    }

    static String eventTypeName(int type) {
        switch (type) {
            case 1: return "ACTIVITY_RESUMED";
            case 2: return "ACTIVITY_PAUSED";
            case 3: return "END_OF_DAY";
            case 4: return "CONTINUE_PREVIOUS_DAY";
            case 5: return "CONFIGURATION_CHANGE";
            case 6: return "SYSTEM_INTERACTION";
            case 7: return "USER_INTERACTION";
            case 8: return "SHORTCUT_INVOCATION";
            case 9: return "CHOOSER_ACTION";
            case 10: return "NOTIFICATION_SEEN";
            case 11: return "STANDBY_BUCKET_CHANGED";
            case 12: return "NOTIFICATION_INTERRUPTION";
            case 13: return "SLICE_PINNED_PRIV";
            case 14: return "SLICE_PINNED";
            case 15: return "SCREEN_INTERACTIVE";
            case 16: return "SCREEN_NON_INTERACTIVE";
            case 17: return "KEYGUARD_SHOWN";
            case 18: return "KEYGUARD_HIDDEN";
            case 19: return "FOREGROUND_SERVICE_START";
            case 20: return "FOREGROUND_SERVICE_STOP";
            case 21: return "CONTINUING_FOREGROUND_SERVICE";
            case 22: return "ROLLOVER_FOREGROUND_SERVICE";
            case 23: return "ACTIVITY_STOPPED";
            case 24: return "ACTIVITY_DESTROYED";
            case 25: return "FLUSH_TO_DISK";
            case 26: return "DEVICE_SHUTDOWN";
            case 27: return "DEVICE_STARTUP";
            case 28: return "USER_UNLOCKED";
            case 29: return "USER_STOPPED";
            case 30: return "LOCUS_ID_SET";
            case 31: return "APP_COMPONENT_USED";
            default: return "TYPE_" + type;
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        return null;
    }

    @Override
    public String getType(Uri uri) {
        return "application/x-ndjson";
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        return null;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        return 0;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }
}
