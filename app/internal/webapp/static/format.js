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
  gmail: "#e06c5f", slack: "#5fb0e0", apple_messages: "#62c98d", whatsapp: "#45c07a",
  apple_notes: "#e8975a", voice_memos: "#e05f9a", calendar: "#e8b45a",
  google_drive: "#d9c95f", contacts: "#7fd0c4", whoop: "#b8d84e", whoop_private: "#a3c93f",
  mutations: "#c46be0", warehouse: "#8b94a1",
  alice_voice_recordings: "#de7eb6", finance: "#6fcf97",
  photos: "#e8a06b",
  claude_code: "#b78ae8", codex: "#9a8ae8", openclaw: "#8aa6e8", pi: "#7b93df", claude_desktop: "#cf8ae8", chatgpt: "#8ae8c9",
  agent_sessions: "#b78ae8", agent_session: "#b78ae8",
};
export function hue(src) { return HUES[src] || "#8b94a1"; }
