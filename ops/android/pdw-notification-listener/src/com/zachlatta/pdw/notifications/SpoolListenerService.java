package com.zachlatta.pdw.notifications;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.Person;
import android.content.pm.ApplicationInfo;
import android.content.pm.PackageManager;
import android.os.Bundle;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.Parcelable;
import android.service.notification.NotificationListenerService;
import android.service.notification.StatusBarNotification;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Writes one JSON file per notification event into files/spool/: "posted"
 * (a new notification or an update whose visible content changed), "removed"
 * (with Android's reason, so a tap is told apart from a swipe), and "active"
 * (what was already showing when the listener connected, so nothing posted
 * while it was not running is lost), and "interruption_filter" (Do Not
 * Disturb turned on, off or changed; also recorded on connect). Each
 * notification event carries how Android ranked it: whether it could make a
 * sound, when it last did, whether Do Not Disturb let it through. What
 * happens to a notification on screen (shown, expanded, tapped, a button
 * pressed) is not visible to a listener; the uploader's tail-system-log reads
 * it from Android's events log. The root uploader
 * (pdw ingest android-notifications) sends the files and deletes them.
 *
 * Each file is written to a temporary name and renamed into place, so the
 * uploader never reads a partial event.
 */
public class SpoolListenerService extends NotificationListenerService {
    private static final String TAG = "PdwNotifications";
    static final int LISTENER_VERSION = 2;
    private static final int MAX_TEXT = 8000;

    private HandlerThread thread;
    private Handler handler;
    // Notification key -> content sha of its last recorded post. An update
    // that changes nothing visible (a progress tick, a re-post) is dropped.
    private final Map<String, String> lastContent = new HashMap<>();

    @Override
    public void onCreate() {
        super.onCreate();
        thread = new HandlerThread("pdw-notification-spool");
        thread.start();
        handler = new Handler(thread.getLooper());
    }

    @Override
    public void onDestroy() {
        if (thread != null) {
            thread.quitSafely();
        }
        super.onDestroy();
    }

    @Override
    public void onListenerConnected() {
        final StatusBarNotification[] active;
        try {
            active = getActiveNotifications();
        } catch (RuntimeException e) {
            Log.w(TAG, "getActiveNotifications failed", e);
            return;
        }
        if (active == null) {
            return;
        }
        final RankingMap ranking = getCurrentRanking();
        final int filter = getCurrentInterruptionFilter();
        handler.post(() -> {
            recordInterruptionFilter(filter, true);
            for (StatusBarNotification sbn : active) {
                record("active", sbn, ranking, -1);
            }
        });
    }

    @Override
    public void onNotificationPosted(StatusBarNotification sbn, RankingMap rankingMap) {
        handler.post(() -> record("posted", sbn, rankingMap, -1));
    }

    @Override
    public void onNotificationRemoved(StatusBarNotification sbn, RankingMap rankingMap, int reason) {
        handler.post(() -> record("removed", sbn, rankingMap, reason));
    }

    @Override
    public void onInterruptionFilterChanged(int interruptionFilter) {
        handler.post(() -> recordInterruptionFilter(interruptionFilter, false));
    }

    private void recordInterruptionFilter(int filter, boolean onConnect) {
        try {
            JSONObject e = new JSONObject();
            long now = System.currentTimeMillis();
            e.put("event_id", UUID.randomUUID().toString());
            e.put("event_type", "interruption_filter");
            e.put("event_at_ms", now);
            e.put("listener_version", LISTENER_VERSION);
            e.put("interruption_filter", filter);
            e.put("interruption_filter_name", filterName(filter));
            e.put("on_connect", onConnect);
            e.put("content_sha256", "");
            write(e);
        } catch (Exception ex) {
            Log.w(TAG, "could not record an interruption filter change", ex);
        }
    }

    private void record(String type, StatusBarNotification sbn, RankingMap rankingMap, int reason) {
        try {
            JSONObject event = describe(type, sbn, rankingMap, reason);
            String sha = event.getString("content_sha256");
            String key = sbn.getKey();
            if ("posted".equals(type) && sha.equals(lastContent.get(key))) {
                return;
            }
            if ("removed".equals(type)) {
                lastContent.remove(key);
            } else {
                lastContent.put(key, sha);
            }
            write(event);
        } catch (Exception e) {
            Log.w(TAG, "could not record a " + type + " notification", e);
        }
    }

    private JSONObject describe(String type, StatusBarNotification sbn, RankingMap rankingMap, int reason)
            throws JSONException {
        Notification n = sbn.getNotification();
        Bundle extras = n.extras != null ? n.extras : new Bundle();
        long now = System.currentTimeMillis();

        JSONObject e = new JSONObject();
        e.put("event_id", UUID.randomUUID().toString());
        e.put("event_type", type);
        e.put("event_at_ms", now);
        e.put("listener_version", LISTENER_VERSION);
        e.put("notification_key", sbn.getKey());
        e.put("package_name", sbn.getPackageName());
        e.put("app_label", appLabel(sbn.getPackageName()));
        e.put("notification_id", sbn.getId());
        e.put("tag", nz(sbn.getTag()));
        e.put("user_id", sbn.getUser() != null ? sbn.getUser().hashCode() : 0);
        e.put("post_time_ms", sbn.getPostTime());
        e.put("when_ms", n.when);
        e.put("channel_id", nz(n.getChannelId()));
        e.put("category", nz(n.category));
        e.put("group_key", nz(sbn.getGroupKey()));
        e.put("group", nz(n.getGroup()));
        e.put("sort_key", nz(n.getSortKey()));
        e.put("is_group_summary", (n.flags & Notification.FLAG_GROUP_SUMMARY) != 0);
        e.put("is_ongoing", sbn.isOngoing());
        e.put("is_clearable", sbn.isClearable());
        e.put("flags", n.flags);
        e.put("visibility", n.visibility);
        e.put("number", n.number);
        e.put("ticker", text(n.tickerText));
        e.put("template", nz(extras.getString(Notification.EXTRA_TEMPLATE)));
        e.put("title", text(extras.getCharSequence(Notification.EXTRA_TITLE)));
        e.put("title_big", text(extras.getCharSequence(Notification.EXTRA_TITLE_BIG)));
        e.put("text", text(extras.getCharSequence(Notification.EXTRA_TEXT)));
        e.put("sub_text", text(extras.getCharSequence(Notification.EXTRA_SUB_TEXT)));
        e.put("big_text", text(extras.getCharSequence(Notification.EXTRA_BIG_TEXT)));
        e.put("info_text", text(extras.getCharSequence(Notification.EXTRA_INFO_TEXT)));
        e.put("summary_text", text(extras.getCharSequence(Notification.EXTRA_SUMMARY_TEXT)));
        e.put("conversation_title", text(extras.getCharSequence(Notification.EXTRA_CONVERSATION_TITLE)));
        e.put("is_group_conversation", extras.getBoolean(Notification.EXTRA_IS_GROUP_CONVERSATION, false));

        JSONArray lines = new JSONArray();
        CharSequence[] textLines = extras.getCharSequenceArray(Notification.EXTRA_TEXT_LINES);
        if (textLines != null) {
            for (CharSequence line : textLines) {
                lines.put(text(line));
            }
        }
        e.put("text_lines", lines);
        e.put("messages", messages(extras));
        e.put("people", people(extras));

        JSONArray actions = new JSONArray();
        if (n.actions != null) {
            for (Notification.Action action : n.actions) {
                if (action != null && action.title != null) {
                    actions.put(text(action.title));
                }
            }
        }
        e.put("actions", actions);

        if (extras.containsKey(Notification.EXTRA_PROGRESS_MAX)) {
            JSONObject progress = new JSONObject();
            progress.put("value", extras.getInt(Notification.EXTRA_PROGRESS, 0));
            progress.put("max", extras.getInt(Notification.EXTRA_PROGRESS_MAX, 0));
            progress.put("indeterminate", extras.getBoolean(Notification.EXTRA_PROGRESS_INDETERMINATE, false));
            e.put("progress", progress);
        }

        Ranking ranking = new Ranking();
        if (rankingMap != null && rankingMap.getRanking(sbn.getKey(), ranking)) {
            e.put("importance", ranking.getImportance());
            NotificationChannel channel = ranking.getChannel();
            e.put("channel_name", channel != null && channel.getName() != null ? text(channel.getName()) : "");
            e.put("is_ambient", ranking.isAmbient());
            e.put("is_conversation", ranking.isConversation());
            e.put("rank", ranking.getRank());
            e.put("matches_interruption_filter", ranking.matchesInterruptionFilter());
            e.put("suppressed_visual_effects", ranking.getSuppressedVisualEffects());
            e.put("last_audibly_alerted_ms", ranking.getLastAudiblyAlertedMillis());
            e.put("user_sentiment", ranking.getUserSentiment());
            e.put("can_bubble", ranking.canBubble());
            e.put("can_show_badge", ranking.canShowBadge());
            e.put("is_suspended", ranking.isSuspended());
            e.put("smart_reply_count", ranking.getSmartReplies() != null ? ranking.getSmartReplies().size() : 0);
            e.put("smart_action_count", ranking.getSmartActions() != null ? ranking.getSmartActions().size() : 0);
            if (channel != null) {
                e.put("channel_importance", channel.getImportance());
                e.put("channel_sound", channel.getSound() != null ? channel.getSound().toString() : "");
                e.put("channel_vibrates", channel.shouldVibrate());
                e.put("channel_lights", channel.shouldShowLights());
                e.put("channel_bypass_dnd", channel.canBypassDnd());
                e.put("channel_lockscreen_visibility", channel.getLockscreenVisibility());
                e.put("channel_show_badge", channel.canShowBadge());
                e.put("channel_group", nz(channel.getGroup()));
            }
        }
        e.put("only_alert_once", (n.flags & Notification.FLAG_ONLY_ALERT_ONCE) != 0);
        e.put("has_full_screen_intent", n.fullScreenIntent != null);
        e.put("has_content_intent", n.contentIntent != null);
        e.put("has_delete_intent", n.deleteIntent != null);
        e.put("timeout_ms", n.getTimeoutAfter());

        if (reason >= 0) {
            e.put("removal_reason", reason);
            e.put("removal_reason_name", reasonName(reason));
        }
        e.put("extras", extrasStrings(extras));
        e.put("content_sha256", contentSha(e));
        return e;
    }

    private JSONArray messages(Bundle extras) throws JSONException {
        JSONArray out = new JSONArray();
        Parcelable[] bundles = extras.getParcelableArray(Notification.EXTRA_MESSAGES);
        if (bundles == null) {
            return out;
        }
        for (Parcelable p : bundles) {
            if (!(p instanceof Bundle)) {
                continue;
            }
            Bundle b = (Bundle) p;
            JSONObject m = new JSONObject();
            m.put("text", text(b.getCharSequence("text")));
            m.put("time_ms", b.getLong("time", 0));
            CharSequence sender = b.getCharSequence("sender");
            Object person = b.get("sender_person");
            if (sender == null && person instanceof Person) {
                sender = ((Person) person).getName();
            }
            m.put("sender", text(sender));
            out.put(m);
        }
        return out;
    }

    private JSONArray people(Bundle extras) {
        JSONArray out = new JSONArray();
        Object list = extras.get(Notification.EXTRA_PEOPLE_LIST);
        if (list instanceof ArrayList) {
            for (Object item : (ArrayList<?>) list) {
                if (item instanceof Person) {
                    Person person = (Person) item;
                    String name = text(person.getName());
                    out.put(!name.isEmpty() ? name : nz(person.getUri()));
                }
            }
        }
        return out;
    }

    private JSONObject extrasStrings(Bundle extras) throws JSONException {
        JSONObject out = new JSONObject();
        for (String key : extras.keySet()) {
            Object value;
            try {
                value = extras.get(key);
            } catch (RuntimeException e) {
                continue;
            }
            if (value instanceof CharSequence || value instanceof Number || value instanceof Boolean) {
                out.put(key, clip(value.toString()));
            } else if (value instanceof CharSequence[]) {
                StringBuilder sb = new StringBuilder();
                for (CharSequence cs : (CharSequence[]) value) {
                    if (sb.length() > 0) {
                        sb.append('\n');
                    }
                    sb.append(cs);
                }
                out.put(key, clip(sb.toString()));
            } else if (value != null) {
                // Icons, bitmaps, intents and parcels: name the type only.
                out.put(key, "<" + value.getClass().getSimpleName() + ">");
            }
        }
        return out;
    }

    // The sha of what a person would see: an update that changes none of it
    // is not a new event.
    private static String contentSha(JSONObject e) {
        String[] fields = {
            "package_name", "channel_id", "title", "title_big", "text", "sub_text", "big_text",
            "info_text", "summary_text", "conversation_title", "text_lines", "messages",
        };
        StringBuilder sb = new StringBuilder();
        for (String field : fields) {
            sb.append(field).append('=').append(e.opt(field)).append('\u0000');
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(sb.toString().getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                hex.append(String.format(Locale.ROOT, "%02x", b));
            }
            return hex.toString();
        } catch (Exception ex) {
            return "";
        }
    }

    private void write(JSONObject event) throws Exception {
        File spool = new File(getFilesDir(), "spool");
        File tmp = new File(spool, ".tmp");
        if (!tmp.isDirectory() && !tmp.mkdirs()) {
            throw new IllegalStateException("cannot create " + tmp);
        }
        String name = String.format(Locale.ROOT, "%013d-%s.json",
                event.getLong("event_at_ms"), event.getString("event_id"));
        File partial = new File(tmp, name);
        try (FileOutputStream out = new FileOutputStream(partial)) {
            out.write(event.toString().getBytes(StandardCharsets.UTF_8));
            out.getFD().sync();
        }
        if (!partial.renameTo(new File(spool, name))) {
            partial.delete();
            throw new IllegalStateException("cannot move " + partial + " into the spool");
        }
    }

    private String appLabel(String packageName) {
        try {
            PackageManager pm = getPackageManager();
            ApplicationInfo info = pm.getApplicationInfo(packageName, 0);
            return text(pm.getApplicationLabel(info));
        } catch (Exception e) {
            return "";
        }
    }

    private static String filterName(int filter) {
        switch (filter) {
            case INTERRUPTION_FILTER_ALL: return "all";
            case INTERRUPTION_FILTER_PRIORITY: return "priority";
            case INTERRUPTION_FILTER_NONE: return "none";
            case INTERRUPTION_FILTER_ALARMS: return "alarms";
            default: return "unknown";
        }
    }

    private static String reasonName(int reason) {
        switch (reason) {
            case 1: return "click";
            case 2: return "cancel";
            case 3: return "cancel_all";
            case 4: return "error";
            case 5: return "package_changed";
            case 6: return "user_stopped";
            case 7: return "package_banned";
            case 8: return "app_cancel";
            case 9: return "app_cancel_all";
            case 10: return "listener_cancel";
            case 11: return "listener_cancel_all";
            case 12: return "group_summary_canceled";
            case 13: return "group_optimization";
            case 14: return "package_suspended";
            case 15: return "profile_turned_off";
            case 16: return "unautobundled";
            case 17: return "channel_banned";
            case 18: return "snoozed";
            case 19: return "timeout";
            case 20: return "channel_removed";
            case 21: return "clear_data";
            case 22: return "assistant_cancel";
            case 23: return "lockdown";
            default: return "reason_" + reason;
        }
    }

    private static String text(CharSequence value) {
        return value == null ? "" : clip(value.toString());
    }

    private static String nz(Object value) {
        return value == null ? "" : value.toString();
    }

    private static String clip(String value) {
        return value.length() > MAX_TEXT ? value.substring(0, MAX_TEXT) : value;
    }
}
