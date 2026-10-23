// Videos play in the card (Zach, voice memo 2026-10-23: "embed videos if
// possible"), not in a browser sheet. A YouTube or Vimeo page plays in that
// site's own embedded player; a video file (an X post's mp4) in a plain
// <video>. Both run in a WebView, which the installed app already has, so
// this ships as an over-the-air update.

import type { FeedMedia } from './rich';

export type VideoSource =
  | { kind: 'youtube'; id: string; start?: number; poster: string; url: string }
  | { kind: 'vimeo'; id: string; poster?: string; url: string }
  | { kind: 'file'; url: string; poster?: string };

const YT_ID = /^[A-Za-z0-9_-]{11}$/;

function parse(url: string): URL | null {
  try {
    return new URL(url);
  } catch {
    return null;
  }
}

// t=90, t=1m30s, start=90.
function seconds(raw: string | null): number | undefined {
  if (!raw) return undefined;
  if (/^\d+$/.test(raw)) return Number(raw) || undefined;
  const m = /^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$/.exec(raw);
  if (!m || !raw) return undefined;
  const total = Number(m[1] ?? 0) * 3600 + Number(m[2] ?? 0) * 60 + Number(m[3] ?? 0);
  return total || undefined;
}

export function youTubeId(url: string | undefined): string {
  const u = url ? parse(url) : null;
  if (!u) return '';
  const host = u.hostname.toLowerCase().replace(/^(www|m|music)\./, '');
  let id = '';
  if (host === 'youtu.be') id = u.pathname.split('/')[1] ?? '';
  else if (host === 'youtube.com' || host === 'youtube-nocookie.com') {
    if (u.pathname === '/watch') id = u.searchParams.get('v') ?? '';
    else {
      const m = /^\/(?:shorts|embed|live|v)\/([^/?#]+)/.exec(u.pathname);
      id = m?.[1] ?? '';
    }
  }
  return YT_ID.test(id) ? id : '';
}

export function vimeoId(url: string | undefined): string {
  const u = url ? parse(url) : null;
  if (!u) return '';
  const host = u.hostname.toLowerCase().replace(/^www\./, '');
  if (host === 'vimeo.com') return /^\/(\d{5,})(?:\/|$)/.exec(u.pathname)?.[1] ?? '';
  if (host === 'player.vimeo.com') return /^\/video\/(\d{5,})/.exec(u.pathname)?.[1] ?? '';
  return '';
}

const FILE = /\.(mp4|m4v|mov|webm|m3u8)(?:[?#]|$)/i;

/** The player for a page or file url, or null when it is not a video. */
export function videoSource(url: string | undefined, poster?: string): VideoSource | null {
  if (!url) return null;
  const yt = youTubeId(url);
  if (yt) {
    const u = parse(url);
    return {
      kind: 'youtube',
      id: yt,
      start: seconds(u?.searchParams.get('t') ?? u?.searchParams.get('start') ?? null),
      poster: poster || `https://i.ytimg.com/vi/${yt}/hqdefault.jpg`,
      url,
    };
  }
  const vimeo = vimeoId(url);
  if (vimeo) return { kind: 'vimeo', id: vimeo, poster, url };
  if (FILE.test(url) || /^https:\/\/video\.twimg\.com\//.test(url)) return { kind: 'file', url, poster };
  return null;
}

/** A media entry's player: a video or gif file, or a video page. */
export function mediaVideo(media: FeedMedia | undefined): VideoSource | null {
  if (!media || media.type === 'image') return null;
  return videoSource(media.url, media.thumbnail_url) ?? { kind: 'file', url: media.url, poster: media.thumbnail_url };
}

/**
 * The video a card plays in place of its picture: its own page when that
 * is a YouTube or Vimeo video (the og:image becomes the poster), else its
 * one video. A carousel of several keeps its pages.
 */
export function cardVideo(item: { url?: string; media?: FeedMedia[]; image_url?: string }): VideoSource | null {
  const media = item.media ?? [];
  const still = media.find((m) => m.type === 'image')?.url ?? item.image_url;
  const page = videoSource(item.url, still);
  if (page && page.kind !== 'file' && media.filter((m) => m.type !== 'image').length === 0 && media.length <= 1) return page;
  if (media.length === 1) return mediaVideo(media[0]);
  return null;
}

function escapeAttr(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
}

// YouTube refuses an embed with no referrer (error 153), so the player is
// an iframe inside a page loaded with an https base url, not the embed url
// loaded directly.
export const EMBED_BASE_URL = 'https://pdw.zachlatta.com/';

/** The page the WebView loads to play a source, starting at once. */
// X's video host (video.twimg.com) refuses a request with a foreign
// referrer (403), so a file plays with none.
export function playerHtml(src: VideoSource): string {
  const head =
    '<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">' +
    (src.kind === 'file' ? '<meta name="referrer" content="no-referrer">' : '') +
    '<style>html,body{margin:0;height:100%;background:#000;overflow:hidden}iframe,video{position:fixed;inset:0;width:100%;height:100%;border:0}</style>' +
    '</head><body>';
  let player: string;
  if (src.kind === 'youtube') {
    const q = `autoplay=1&playsinline=1&rel=0&modestbranding=1${src.start ? `&start=${src.start}` : ''}`;
    player =
      `<iframe src="https://www.youtube.com/embed/${src.id}?${q}" referrerpolicy="strict-origin-when-cross-origin" ` +
      'allow="autoplay; encrypted-media; picture-in-picture; fullscreen" allowfullscreen></iframe>';
  } else if (src.kind === 'vimeo') {
    player =
      `<iframe src="https://player.vimeo.com/video/${src.id}?autoplay=1&playsinline=1" ` +
      'allow="autoplay; fullscreen; picture-in-picture" allowfullscreen></iframe>';
  } else {
    const poster = src.poster ? ` poster="${escapeAttr(src.poster)}"` : '';
    player = `<video src="${escapeAttr(src.url)}"${poster} controls autoplay playsinline></video>`;
  }
  return `${head}${player}</body></html>`;
}
