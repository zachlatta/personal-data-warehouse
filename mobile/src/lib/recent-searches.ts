// The last few searches, newest first, so the Search screen opens on
// something to tap instead of an empty page (Superhuman shows recent
// searches under its search box). The file on the phone is
// recent-searches-store.ts; it is removed on Disconnect.
export const MAX_RECENT = 8;

// Newest first; the same words in another case or spacing count once.
export function addRecent(list: string[], query: string, max: number = MAX_RECENT): string[] {
  const q = query.trim().replace(/\s+/g, ' ');
  if (!q) return list;
  const lower = q.toLowerCase();
  return [q, ...list.filter((item) => item.toLowerCase() !== lower)].slice(0, max);
}

export function parseRecent(text: string): string[] {
  try {
    const value = JSON.parse(text);
    if (!Array.isArray(value)) return [];
    return value.filter((item): item is string => typeof item === 'string' && item.trim() !== '').slice(0, MAX_RECENT);
  } catch {
    return [];
  }
}
