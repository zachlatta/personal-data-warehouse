// Rich cards: photo/video carousels (after Instagram) and embedded X posts
// (after X), shown in the card itself instead of as a link out. The server
// fills an X post's embed when an agent pushes its x.com url.

export type FeedMedia = {
  type: 'image' | 'video' | 'gif';
  url: string;
  thumbnail_url?: string;
  width?: number;
  height?: number;
  alt?: string;
  duration_sec?: number;
};

export type FeedEmbed = {
  type: 'x_post';
  url?: string;
  id?: string;
  author_name?: string;
  author_handle?: string;
  author_avatar_url?: string;
  author_verified?: boolean;
  text?: string;
  created_at?: string;
  replies?: number;
  reposts?: number;
  likes?: number;
  views?: number;
  bookmarks?: number;
  replying_to?: string;
  media?: FeedMedia[];
  quote?: FeedEmbed;
  thread?: FeedEmbed[];
};

// 1234 -> 1.2K, 2500000 -> 2.5M, the way X prints counts.
export function compactCount(n: number | undefined): string {
  if (!n || n < 0) return '';
  if (n < 1000) return String(n);
  const [div, unit] = n < 1e6 ? [1e3, 'K'] : n < 1e9 ? [1e6, 'M'] : [1e9, 'B'];
  const value = n / div;
  const text = value < 10 ? (Math.floor(value * 10) / 10).toFixed(1) : String(Math.floor(value));
  return text.replace(/\.0$/, '') + unit;
}

// X/Instagram timestamps: 45s, 12m, 3h, 4d, then Oct 5 (or Oct 5, 2024).
export function shortAge(iso: string | undefined, now: Date = new Date()): string {
  if (!iso) return '';
  const date = new Date(iso);
  const t = date.getTime();
  if (Number.isNaN(t)) return '';
  const s = Math.max(0, Math.floor((now.getTime() - t) / 1000));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m`;
  if (s < 86400) return `${Math.floor(s / 3600)}h`;
  if (s < 7 * 86400) return `${Math.floor(s / 86400)}d`;
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const label = `${months[date.getUTCMonth()]} ${date.getUTCDate()}`;
  return date.getUTCFullYear() === now.getUTCFullYear() ? label : `${label}, ${date.getUTCFullYear()}`;
}

export type PostSegment = { kind: 'text' | 'url' | 'mention' | 'hashtag'; text: string; href?: string };

const TOKEN = /(https?:\/\/[^\s]+)|(@[A-Za-z0-9_]{1,15})|(#[\p{L}\p{N}_]+)/gu;

// Splits post text into plain runs and tappable urls, @mentions and #tags.
export function postSegments(text: string): PostSegment[] {
  const out: PostSegment[] = [];
  let last = 0;
  for (const match of text.matchAll(TOKEN)) {
    const at = match.index ?? 0;
    if (at > last) out.push({ kind: 'text', text: text.slice(last, at) });
    const [token, url, mention] = match;
    if (url) {
      const clean = url.replace(/[).,!?:;'"]+$/, '');
      const display = clean.replace(/^https?:\/\/(www\.)?/, '');
      out.push({ kind: 'url', text: display.length > 32 ? `${display.slice(0, 31)}…` : display, href: clean });
      if (clean.length < url.length) out.push({ kind: 'text', text: url.slice(clean.length) });
    } else if (mention) {
      out.push({ kind: 'mention', text: token, href: `https://x.com/${token.slice(1)}` });
    } else {
      out.push({ kind: 'hashtag', text: token, href: `https://x.com/hashtag/${encodeURIComponent(token.slice(1))}` });
    }
    last = at + token.length;
  }
  if (last < text.length) out.push({ kind: 'text', text: text.slice(last) });
  return out;
}

// X lays out 1-4 media as: one full; two side by side; three as one tall
// left and two stacked right; four as a 2x2 grid. Rows of columns of indexes.
export function mediaGrid(count: number): number[][][] {
  switch (Math.min(count, 4)) {
    case 0:
      return [];
    case 1:
      return [[[0]]];
    case 2:
      return [[[0], [1]]];
    case 3:
      return [[[0], [1, 2]]];
    default:
      return [[[0], [1]], [[2], [3]]];
  }
}

// The single photo's shape, clamped the way Instagram clamps a post:
// between 4:5 portrait and 1.91:1 landscape.
export function mediaAspect(media: FeedMedia | undefined, fallback = 1.91): number {
  if (!media?.width || !media?.height) return fallback;
  return Math.min(1.91, Math.max(0.8, media.width / media.height));
}

export function videoLength(sec: number | undefined): string {
  if (!sec || sec <= 0) return '';
  const total = Math.round(sec);
  return `${Math.floor(total / 60)}:${String(total % 60).padStart(2, '0')}`;
}

const AVATAR_COLORS = ['#E4405F', '#1D9BF0', '#F77737', '#833AB4', '#00BA7C', '#F91880', '#7856FF', '#FFAD1F'];

// A stable color per source for its letter avatar.
export function avatarColor(key: string): string {
  let h = 0;
  for (const ch of key) h = (h * 31 + ch.charCodeAt(0)) >>> 0;
  return AVATAR_COLORS[h % AVATAR_COLORS.length];
}

export function hostOf(url: string | undefined): string {
  const match = url ? /^https?:\/\/([^/?#]+)/i.exec(url) : null;
  return match ? match[1].toLowerCase().replace(/^www\./, '') : '';
}

export type SourceBubble = { source: string; label: string; total: number; unseen: number };

// The rail of sources across the top (Instagram's stories row): one bubble
// per source in feed order, unseen ones first, each with a ring while it
// still has cards he has not seen.
export function sourceBubbles(
  items: { source: string; site?: string; embed?: FeedEmbed; engagement: { impressions: number } }[],
): SourceBubble[] {
  const by = new Map<string, SourceBubble>();
  for (const item of items) {
    const bubble = by.get(item.source) ?? { source: item.source, label: item.source, total: 0, unseen: 0 };
    bubble.total += 1;
    if (item.engagement.impressions === 0) bubble.unseen += 1;
    by.set(item.source, bubble);
  }
  return [...by.values()].sort((a, b) => Number(b.unseen > 0) - Number(a.unseen > 0));
}
