// Videos play in the card (Zach, voice memo 2026-10-23: "embed videos if
// possible"), not in a browser sheet. A YouTube, Vimeo, TikTok or Instagram
// page plays in that site's own embedded player; a video file (an X post's
// mp4) in a plain <video>. Both run in a WebView, which the installed app already has, so
// this ships as an over-the-air update.

import type { FeedEmbed, FeedMedia } from './rich';

export type VideoSource =
  | { kind: 'youtube'; id: string; start?: number; poster: string; url: string }
  | { kind: 'vimeo'; id: string; poster?: string; url: string }
  | { kind: 'tiktok'; id: string; poster?: string; url: string }
  | { kind: 'instagram'; code: string; path: 'p' | 'reel' | 'tv'; poster?: string; url: string }
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

// tiktok.com/@someone/video/<id>, or the embed and player urls.
export function tikTokId(url: string | undefined): string {
  const u = url ? parse(url) : null;
  if (!u) return '';
  const host = u.hostname.toLowerCase().replace(/^(www|m)\./, '');
  if (host !== 'tiktok.com') return '';
  return /^\/(?:@[^/]+\/video|embed(?:\/v2)?|player\/v1)\/(\d{15,21})(?:\/|$)/.exec(u.pathname)?.[1] ?? '';
}

// instagram.com/reel/<code>, /p/<code>, /tv/<code> (also under a user's name).
export function instagramPost(url: string | undefined): { code: string; path: 'p' | 'reel' | 'tv' } | null {
  const u = url ? parse(url) : null;
  if (!u) return null;
  const host = u.hostname.toLowerCase().replace(/^(www|m)\./, '');
  if (host !== 'instagram.com') return null;
  const m = /^\/(?:[A-Za-z0-9._]+\/)?(p|reels?|tv)\/([A-Za-z0-9_-]{5,})(?:\/|$)/.exec(u.pathname);
  if (!m) return null;
  return { code: m[2], path: m[1] === 'p' ? 'p' : m[1] === 'tv' ? 'tv' : 'reel' };
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
  const tiktok = tikTokId(url);
  if (tiktok) return { kind: 'tiktok', id: tiktok, poster, url };
  // A /p/ link is usually photos, so only reels (and old IGTV) are players.
  const ig = instagramPost(url);
  if (ig && ig.path !== 'p') return { kind: 'instagram', ...ig, poster, url };
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
 * is a video page (the og:image becomes the poster), else its
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

/**
 * The player key a feed card's video plays under, or null when the card has
 * none to play in place. These match the keys feed-card gives its players: the
 * card's own video or an X post's lone video is the item id; a quoted post's
 * lone video is item id + ":quote".
 */
export function autoplayKey(item: {
  item_id: string;
  url?: string;
  media?: FeedMedia[];
  image_url?: string;
  embed?: FeedEmbed;
}): string | null {
  const embed = item.embed?.type === 'x_post' ? item.embed : undefined;
  if (embed) {
    if (embed.media?.length === 1 && mediaVideo(embed.media[0])) return item.item_id;
    if (embed.quote?.media?.length === 1 && mediaVideo(embed.quote.media[0])) return `${item.item_id}:quote`;
    return null;
  }
  return cardVideo(item) ? item.item_id : null;
}

/** The player's shape: phone video upright like Instagram's feed, files square, the rest 16:9. */
export function videoAspect(src: VideoSource): number {
  if (src.kind === 'file') return 1;
  if (src.kind === 'tiktok' || src.kind === 'instagram') return 4 / 5;
  return 16 / 9;
}

// Posts "fullscreen" when the player goes full screen and "inline" when it
// comes back, so the feed leaves a full-screen video alone (video-player.tsx).
const FULLSCREEN_JS =
  'function pdwFs(){window.ReactNativeWebView&&window.ReactNativeWebView.postMessage(document.fullscreenElement||document.webkitFullscreenElement?"fullscreen":"inline")}' +
  'document.addEventListener("fullscreenchange",pdwFs);document.addEventListener("webkitfullscreenchange",pdwFs);';

/**
 * Instagram's embed page refuses to load in a frame (X-Frame-Options: DENY),
 * so the WebView loads it as the page itself; this is that page's url.
 */
export function instagramEmbedUrl(src: { code: string; path: 'p' | 'reel' | 'tv' }): string {
  return `https://www.instagram.com/${src.path}/${src.code}/embed/`;
}

/**
 * Script run in Instagram's embed page: once its video exists, fill the
 * frame with it (Instagram's header and footer hidden), give it controls for
 * the sound, play it, and say "playing" once it draws. A photo post has no
 * video and stays as Instagram shows it.
 */
export function instagramPlayerScript({ muted = false }: { muted?: boolean } = {}): string {
  return (
    `(function(){${FULLSCREEN_JS}var n=0;var t=setInterval(function(){var v=document.querySelector("video");` +
    'if(!v){if(++n>40){clearInterval(t);window.ReactNativeWebView&&window.ReactNativeWebView.postMessage("playing")}return}' +
    'clearInterval(t);document.documentElement.style.background="#000";document.body.style.background="#000";' +
    'v.style.cssText="position:fixed;inset:0;width:100%;height:100%;object-fit:cover;z-index:2147483647;background:#000";' +
    'v.setAttribute("playsinline","");v.controls=true;v.loop=true;' +
    `v.muted=${muted ? 'true' : 'false'};` +
    'v.addEventListener("playing",function(){window.ReactNativeWebView&&window.ReactNativeWebView.postMessage("playing")},{once:true});' +
    'window.pdwSetMuted=function(m){v.muted=m;if(!m&&v.paused)v.play()};' +
    'var p=v.play();p&&p.catch(function(){v.muted=true;v.play()})},250)})();true;'
  );
}

/**
 * Script the app injects into a playing player to turn its sound on or off
 * (Zach, voice memo 2026-10-24: autoplay stays muted, "but I want to have
 * like a toggle where I can turn on audio for it"). Every player page
 * defines window.pdwSetMuted.
 */
export function setMutedScript(muted: boolean): string {
  return `window.pdwSetMuted&&window.pdwSetMuted(${muted ? 'true' : 'false'});true;`;
}

// window.pdwSetMuted for a page whose player is a site's iframe: each site
// takes commands as messages to its frame.
function iframeSoundScript(kind: 'youtube' | 'vimeo' | 'tiktok'): string {
  const send =
    kind === 'youtube'
      ? 'f({event:"command",func:m?"mute":"unMute",args:[]});if(!m){f({event:"command",func:"setVolume",args:[100]});f({event:"command",func:"playVideo",args:[]})}'
      : kind === 'vimeo'
        ? 'f({method:"setMuted",value:m});if(!m)f({method:"setVolume",value:1})'
        : 'f({type:m?"mute":"unMute","x-tiktok-player":true})';
  return (
    '<script>window.pdwSetMuted=function(m){var w=document.querySelector("iframe").contentWindow;' +
    `function f(o){w.postMessage(${kind === 'tiktok' ? 'o' : 'JSON.stringify(o)'},"*")}${send}};</script>`
  );
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
// referrer (403), so a file plays with none. muted is the autoplay a
// scroll starts, as X and Instagram do: sound off until he turns it on.
// The page is transparent until the video draws, so the app's poster and
// spinner show through while it loads, and it posts "playing" once it does.
export function playerHtml(src: VideoSource, { muted = false }: { muted?: boolean } = {}): string {
  const head =
    '<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">' +
    (src.kind === 'file' ? '<meta name="referrer" content="no-referrer">' : '') +
    '<style>html,body{margin:0;height:100%;background:transparent;overflow:hidden}iframe,video{position:fixed;inset:0;width:100%;height:100%;border:0}</style>' +
    '</head><body>';
  let player: string;
  if (src.kind === 'youtube') {
    // enablejsapi lets the app's sound button command the player.
    const origin = encodeURIComponent(EMBED_BASE_URL.replace(/\/$/, ''));
    const q = `autoplay=1&playsinline=1&rel=0&modestbranding=1&enablejsapi=1&origin=${origin}${muted ? '&mute=1' : ''}${src.start ? `&start=${src.start}` : ''}`;
    player =
      `<iframe src="https://www.youtube.com/embed/${src.id}?${q}" referrerpolicy="strict-origin-when-cross-origin" ` +
      'allow="autoplay; encrypted-media; picture-in-picture; fullscreen" allowfullscreen></iframe>' +
      iframeSoundScript('youtube');
  } else if (src.kind === 'tiktok') {
    player =
      `<iframe src="https://www.tiktok.com/player/v1/${src.id}?autoplay=1&muted=${muted ? 1 : 0}&loop=1&rel=0&description=0&music_info=0" ` +
      'allow="autoplay; fullscreen; picture-in-picture" allowfullscreen></iframe>' +
      iframeSoundScript('tiktok');
  } else if (src.kind === 'instagram') {
    // Never loaded: the WebView opens instagramEmbedUrl instead.
    player = '';
  } else if (src.kind === 'vimeo') {
    player =
      `<iframe src="https://player.vimeo.com/video/${src.id}?autoplay=1&playsinline=1${muted ? '&muted=1' : ''}" ` +
      'allow="autoplay; fullscreen; picture-in-picture" allowfullscreen></iframe>' +
      iframeSoundScript('vimeo');
  } else {
    const poster = src.poster ? ` poster="${escapeAttr(src.poster)}"` : '';
    player =
      `<video src="${escapeAttr(src.url)}"${poster} controls autoplay playsinline preload="auto"${muted ? ' muted' : ''}></video>` +
      '<script>var v=document.querySelector("video");v.addEventListener("playing",function(){v.style.background="#000";' +
      'window.ReactNativeWebView&&window.ReactNativeWebView.postMessage("playing")},{once:true});' +
      'window.pdwSetMuted=function(m){v.muted=m;if(!m&&v.paused)v.play()};</script>';
  }
  return `${head}${player}<script>${FULLSCREEN_JS}</script></body></html>`;
}
