// Pure formatting helpers (no DOM) — unit-tested under node.

export const WEEKDAYS = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

export function pad2(n) { return (n < 10 ? "0" : "") + n; }

// All display times are the browser's local timezone.
export function fmtTime(ts) {
  if (!ts) return "—";
  const d = new Date(ts);
  if (isNaN(d.getTime())) return String(ts);
  return pad2(d.getHours()) + ":" + pad2(d.getMinutes());
}

export function dayOf(ts) {
  if (!ts) return "unknown";
  const d = new Date(ts);
  if (isNaN(d.getTime())) return "unknown";
  return d.getFullYear() + "-" + pad2(d.getMonth() + 1) + "-" + pad2(d.getDate());
}

export function fmtFull(ts) {
  if (!ts) return "";
  const d = new Date(ts);
  return isNaN(d.getTime()) ? String(ts) : d.toLocaleString();
}

// "just now", "4m ago", "3h ago", "2d ago", then the date.
export function formatWhen(ts, now = new Date()) {
  if (!ts) return "";
  const d = new Date(ts);
  if (isNaN(d.getTime())) return String(ts);
  const seconds = Math.round((now.getTime() - d.getTime()) / 1000);
  if (seconds < 0) return d.toLocaleString();
  if (seconds < 60) return "just now";
  if (seconds < 3600) return Math.floor(seconds / 60) + "m ago";
  if (seconds < 86400) return Math.floor(seconds / 3600) + "h ago";
  if (seconds < 7 * 86400) return Math.floor(seconds / 86400) + "d ago";
  return d.toLocaleDateString();
}

// The warehouse stores "absent" as the epoch, never NULL.
export function isReal(ts) { return !!ts && String(ts).slice(0, 4) !== "1970"; }

export function pretty(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (Array.isArray(value) && value.every((v) => typeof v !== "object")) return value.join(", ");
  return JSON.stringify(value, null, 2);
}

export function truncate(value, max) {
  const text = String(value || "");
  if (text.length <= max) return text;
  return text.slice(0, Math.max(0, max - 1)) + "…";
}

const HUES = {
  gmail: "#e5484d", slack: "#7c5cc4", apple_messages: "#2f9e5b", whatsapp: "#22a39a",
  apple_notes: "#d9a21b", voice_memos: "#d6409f", calendar: "#3b63d4",
  google_drive: "#c98a10", contacts: "#0f8a8a", whoop: "#5f9a1c", whoop_private: "#4d7d17",
  mutations: "#8a5bd6", warehouse: "#8f8b86",
  alice_voice_recordings: "#c2477f", finance: "#1f9d55",
  photos: "#e2702a",
  claude_code: "#d0743f", codex: "#2d7f86", openclaw: "#3d7be0", pi: "#5458d6", muse_code: "#b8457a", argon: "#1690b8", claude_desktop: "#b85f2c", chatgpt: "#139a7b", muse: "#d6456a",
  agent_sessions: "#d0743f", agent_session: "#d0743f",
};
export function hue(src) { return HUES[src] || "#8f8b86"; }
