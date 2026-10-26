// What the phone keeps so the feed reads offline (Zach, 2026-10-25: "Have
// pdw feed download it's items and articles so stuff is ready to read
// offline"). While online, every saved card and the next AHEAD cards of the
// feed get their pictures into expo-image's disk cache, and the cards with
// an article get the article and its pictures into Documents/pdw-offline.
// This file is the plan and the bookkeeping, with no I/O (offline.ts does
// the downloads), so node --test runs it.
import type { FeedPage } from './api';
import type { FeedEmbed, FeedMedia } from './rich';

// The feed page's first cards, in served order (unseen first, then the
// resurfaced ones), seen or not.
export const AHEAD = 40;
export const MAX_STORE_BYTES = 250 * 1024 * 1024;
export const MAX_AGE_MS = 14 * 24 * 60 * 60 * 1000;
// A page with no article (a 422) or a failed read is not asked again for a day.
export const RETRY_FAILED_MS = 24 * 60 * 60 * 1000;
// One article's pictures go into its page as data: URIs, through the bridge
// into the WebView, so they are capped in count, each and in total.
export const MAX_IMAGES = 15;
export const MAX_IMAGE_BYTES = 3 * 1024 * 1024;
export const MAX_ARTICLE_IMAGE_BYTES = 8 * 1024 * 1024;
// The srcset width chosen: the smallest at least this wide.
const TARGET_WIDTH = 1080;

export type StoredImage = { file: string; bytes: number; type: string };

export type OfflineEntry = {
  url: string;
  // The article's JSON file in the store; '' when the last read failed
  // (failed_at says when).
  file: string;
  failed_at: number;
  // Keyed by the picture's url as chosen by imageChoice.
  images: Record<string, StoredImage>;
  bytes: number;
  saved_at: number;
  used_at: number;
};

export type OfflineIndex = { v: 1; entries: Record<string, OfflineEntry> };

export function emptyIndex(): OfflineIndex {
  return { v: 1, entries: {} };
}

export function parseIndex(text: string | null): OfflineIndex {
  try {
    const parsed = text ? JSON.parse(text) : null;
    if (parsed?.v === 1 && parsed.entries && typeof parsed.entries === 'object') return parsed as OfflineIndex;
  } catch {}
  return emptyIndex();
}

/** Every saved card, then the first `ahead` cards of the feed, once each. */
export function plannedCards<T extends { item_id: string }>(saved: T[], items: T[], ahead = AHEAD): T[] {
  const seen = new Set<string>();
  const out: T[] = [];
  for (const item of [...saved, ...items.slice(0, ahead)]) {
    if (seen.has(item.item_id)) continue;
    seen.add(item.item_id);
    out.push(item);
  }
  return out;
}

/** The round source icon a card shows beside its headline (rich-views SourceAvatar). */
export function faviconUrl(url: string | undefined): string {
  const match = url ? /^https?:\/\/([^/?#]+)/i.exec(url) : null;
  const host = match ? match[1].toLowerCase().replace(/^www\./, '') : '';
  return host ? `https://icons.duckduckgo.com/ip3/${host}.ico` : '';
}

type MediaCard = { url?: string; image_url?: string; media?: FeedMedia[]; embed?: FeedEmbed | null };

function mediaPictures(media: FeedMedia[] | undefined): string[] {
  // A video or gif file is not downloaded, only its poster.
  return (media ?? []).flatMap((m) => [m.type === 'image' ? m.url : '', m.thumbnail_url ?? '']);
}

function embedPictures(embed: FeedEmbed | null | undefined): string[] {
  if (!embed) return [];
  return [
    embed.author_avatar_url ?? '',
    ...mediaPictures(embed.media),
    ...embedPictures(embed.quote),
    ...(embed.thread ?? []).flatMap((post) => embedPictures(post)),
  ];
}

/** The pictures a card and its detail view show: photos, posters, an X post's avatar and media, the source icon. */
export function cardMediaUrls(item: MediaCard): string[] {
  const all = [item.image_url ?? '', ...mediaPictures(item.media), ...embedPictures(item.embed), faviconUrl(item.url)];
  return [...new Set(all.filter((u) => /^https?:\/\//i.test(u)))];
}

/** The article urls still to download: not stored, and not failed in the last day. */
export function planArticles(urls: (string | null)[], index: OfflineIndex, now: number): string[] {
  const out: string[] = [];
  for (const url of urls) {
    if (!url || out.includes(url)) continue;
    const entry = index.entries[url];
    if (entry?.file) continue;
    if (entry && now - entry.failed_at < RETRY_FAILED_MS) continue;
    out.push(url);
  }
  return out;
}

function decodeEntities(s: string): string {
  return s.replace(/&(amp|quot|#39|#x27|lt|gt);/g, (_, e: string) => ({ amp: '&', quot: '"', '#39': "'", '#x27': "'", lt: '<', gt: '>' })[e] as string);
}

function attr(tag: string, name: string): string {
  const m = new RegExp(`\\s${name}\\s*=\\s*(?:"([^"]*)"|'([^']*)')`, 'i').exec(tag);
  return m ? decodeEntities(m[1] ?? m[2] ?? '') : '';
}

/** The one url an <img> tag is downloaded as: from its srcset, the smallest at least 1080 wide (else the widest), else its src. */
export function imageChoice(tag: string): string {
  const candidates = attr(tag, 'srcset')
    .split(',')
    .map((s) => s.trim().split(/\s+/))
    .filter(([u]) => u && /^https?:\/\//i.test(u))
    .map(([u, d]) => ({ url: u, width: d && /^\d+w$/.test(d) ? Number(d.slice(0, -1)) : 0 }));
  const sized = candidates.filter((c) => c.width > 0).sort((a, b) => a.width - b.width);
  if (sized.length) return (sized.find((c) => c.width >= TARGET_WIDTH) ?? sized[sized.length - 1]).url;
  const src = attr(tag, 'src');
  if (/^https?:\/\//i.test(src)) return src;
  return candidates[0]?.url ?? '';
}

const IMG = /<img\b[^>]*>/gi;

/** The article's pictures to download, in page order, at most MAX_IMAGES. */
export function pickArticleImages(html: string): string[] {
  const out: string[] = [];
  for (const tag of html.match(IMG) ?? []) {
    const url = imageChoice(tag);
    if (url && !out.includes(url)) out.push(url);
    if (out.length >= MAX_IMAGES) break;
  }
  return out;
}

/**
 * The size limits for one article's pictures, applied as they download:
 * each under MAX_IMAGE_BYTES, together under MAX_ARTICLE_IMAGE_BYTES. A
 * picture past either stays a network image (offline it is hidden).
 */
export class ImageBudget {
  used = 0;
  admit(bytes: number): boolean {
    if (!(bytes > 0) || bytes > MAX_IMAGE_BYTES || this.used + bytes > MAX_ARTICLE_IMAGE_BYTES) return false;
    this.used += bytes;
    return true;
  }
}

/**
 * The article html with each downloaded picture as a data: URI (dataUris
 * is keyed by imageChoice). Its srcset and sizes go, and so do the
 * <source>s of a <picture> around it, so the WebView reads the local copy.
 * A picture not downloaded is left as it was.
 */
export function localizeArticle(html: string, dataUris: Record<string, string>): string {
  const swap = (tag: string): string | null => {
    const uri = dataUris[imageChoice(tag)];
    if (!uri) return null;
    const rest = tag.replace(/\s(src|srcset|sizes)\s*=\s*(?:"[^"]*"|'[^']*')/gi, '').replace(/^<img\b/i, '');
    return `<img src="${uri}"${rest}`;
  };
  const pictures = html.replace(/<picture\b[^>]*>[\s\S]*?<\/picture>/gi, (block) => {
    const tag = block.match(IMG)?.[0];
    const local = tag ? swap(tag) : null;
    return local && tag ? block.replace(/<source\b[^>]*>(\s*<\/source>)?/gi, '').replace(tag, local) : block;
  });
  return pictures.replace(IMG, (tag) => (tag.startsWith(`<img src="data:`) ? tag : (swap(tag) ?? tag)));
}

/** A fetch that never reached the server (no connection, DNS, timeout), as against an HTTP error. */
export function isOfflineError(e: unknown): boolean {
  return e instanceof TypeError || (e instanceof Error && /network request failed|network|offline|timed? ?out/i.test(e.message) && !('status' in e));
}

/**
 * The entries to delete: older than MAX_AGE_MS and not among the current
 * cards, then, while the store is over maxBytes, the least recently used,
 * saved cards last. keep are the current cards' urls; saved the saved ones.
 */
export function evict(index: OfflineIndex, keep: Set<string>, saved: Set<string>, now: number, maxBytes = MAX_STORE_BYTES): string[] {
  const entries = Object.values(index.entries);
  const gone = new Set<string>();
  for (const e of entries) {
    if (!keep.has(e.url) && !saved.has(e.url) && now - Math.max(e.saved_at, e.used_at) > MAX_AGE_MS) gone.add(e.url);
  }
  let total = entries.filter((e) => !gone.has(e.url)).reduce((sum, e) => sum + e.bytes, 0);
  const order = entries
    .filter((e) => !gone.has(e.url))
    .sort((a, b) => Number(saved.has(a.url)) - Number(saved.has(b.url)) || Math.max(a.used_at, a.saved_at) - Math.max(b.used_at, b.saved_at));
  for (const e of order) {
    if (total <= maxBytes) break;
    gone.add(e.url);
    total -= e.bytes;
  }
  return [...gone];
}

export function storeBytes(index: OfflineIndex): number {
  return Object.values(index.entries).reduce((sum, e) => sum + e.bytes, 0);
}

/** A short file name for a url (two 32-bit FNV-1a hashes). */
export function fileKey(url: string): string {
  let a = 0x811c9dc5;
  let b = 0x01000193 ^ url.length;
  for (let i = 0; i < url.length; i++) {
    const c = url.charCodeAt(i);
    a = Math.imul(a ^ c, 0x01000193) >>> 0;
    b = Math.imul(b ^ c, 0x5bd1e995) >>> 0;
  }
  return a.toString(16).padStart(8, '0') + b.toString(16).padStart(8, '0');
}

// The feed page file (feed-cache.ts): a cold start paints it when it is
// under 3 days old; offline, a page up to 14 days old beats nothing.
export const COLD_START_MAX_AGE_MS = 3 * 24 * 60 * 60 * 1000;
export const OFFLINE_PAGE_MAX_AGE_MS = MAX_AGE_MS;
const FEED_CACHE_VERSION = 1;

export type CachedPage = { page: FeedPage; at: number };

export function parseFeedCache(text: string, now: number, maxAgeMs: number): CachedPage | null {
  try {
    const parsed = JSON.parse(text) as { v?: number; at?: number; page?: FeedPage };
    if (parsed.v !== FEED_CACHE_VERSION || !parsed.page || !Array.isArray(parsed.page.items)) return null;
    if (typeof parsed.at !== 'number' || now - parsed.at > maxAgeMs) return null;
    return { page: parsed.page, at: parsed.at };
  } catch {
    return null;
  }
}

export function feedCacheText(page: FeedPage, now: number): string {
  return JSON.stringify({ v: FEED_CACHE_VERSION, at: now, page });
}
