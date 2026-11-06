// What a Feed card says, as plain data. Ported from the phone
// (mobile/src/features/feed/: markdown.ts, links.ts, card-context.ts, rich.ts,
// video.ts, article.ts) because there is no build step here. feed.js turns it
// into text nodes and links only: nothing a card carries ever becomes markup,
// except the two documents at the end, which run inside their own frames.

/** The url if it is http(s), else "": the one gate every link and picture goes through. */
export function safeUrl(value) {
  if (!value) return "";
  try {
    const url = new URL(value);
    return url.protocol === "http:" || url.protocol === "https:" ? url.href : "";
  } catch (e) {
    return "";
  }
}

const INLINE = /(\*\*([^*]+)\*\*|__([^_]+)__|\*([^*\s][^*]*)\*|_([^_\s][^_]*)_|`([^`]+)`|\[([^\]]+)\]\(([^)\s]+)\))/g;

export function parseInline(text) {
  const spans = [];
  let last = 0;
  for (const match of text.matchAll(INLINE)) {
    if (match.index > last) spans.push({ text: text.slice(last, match.index) });
    if (match[2] || match[3]) spans.push({ text: match[2] || match[3], bold: true });
    else if (match[4] || match[5]) spans.push({ text: match[4] || match[5], italic: true });
    else if (match[6]) spans.push({ text: match[6], code: true });
    else if (match[7]) spans.push({ text: match[7], url: match[8] });
    last = match.index + match[0].length;
  }
  if (last < text.length) spans.push({ text: text.slice(last) });
  return spans;
}

export function parseMarkdown(markdown) {
  const blocks = [];
  const lines = markdown.replace(/\r\n?/g, "\n").split("\n");
  let paragraph = [];
  const endParagraph = () => {
    if (paragraph.length) blocks.push({ kind: "paragraph", spans: parseInline(paragraph.join(" ")) });
    paragraph = [];
  };
  for (let i = 0; i < lines.length; i++) {
    const trimmed = lines[i].trim();
    if (trimmed.startsWith("```")) {
      endParagraph();
      const code = [];
      for (i++; i < lines.length && !lines[i].trim().startsWith("```"); i++) code.push(lines[i]);
      blocks.push({ kind: "code", text: code.join("\n") });
      continue;
    }
    if (trimmed === "") { endParagraph(); continue; }
    const heading = /^(#{1,6})\s+(.*)$/.exec(trimmed);
    const bullet = /^[-*+]\s+(.*)$/.exec(trimmed);
    const ordered = /^(\d+)[.)]\s+(.*)$/.exec(trimmed);
    if (heading) { endParagraph(); blocks.push({ kind: "heading", spans: parseInline(heading[2]) }); }
    else if (bullet) { endParagraph(); blocks.push({ kind: "bullet", spans: parseInline(bullet[1]) }); }
    else if (ordered) { endParagraph(); blocks.push({ kind: "bullet", spans: parseInline(ordered[2]), ordered: ordered[1] }); }
    else if (trimmed.startsWith(">")) { endParagraph(); blocks.push({ kind: "quote", spans: parseInline(trimmed.replace(/^>\s?/, "")) }); }
    else paragraph.push(trimmed);
  }
  endParagraph();
  return blocks;
}

function hostOf(url) {
  const match = /^https?:\/\/([^/?#]+)/i.exec(url);
  return match ? match[1].toLowerCase().replace(/^www\./, "") : "";
}

// Every place a card points to, main link first. index is what an open event
// records: 0 is the main url, 1.. the extra links in the card's own order.
export function cardLinks(item) {
  const out = [];
  const main = safeUrl(item.url);
  if (main) out.push({ url: main, label: item.site || hostOf(main), index: 0 });
  (item.links || []).forEach((link, i) => {
    const url = safeUrl(link.url);
    if (url) out.push({ url, label: link.title || link.site || hostOf(url), index: i + 1 });
  });
  return out;
}

const HN_LINE = /^\s*\**(\d[\d,]*) points\**\s*·\s*(\d[\d,]*) comments? on Hacker News\s*·\s*(?:\[discussion\]\(([^)\s]+)\)|discussion)\s*$/im;
const MUSE_KIND = /^\s*\*([^*\n]+)\*\s*(?:\n|$)/;
const FOR_YOU = /^\s*\*\*For you:\*\*\s*/i;
const MUSE_NAME = "Muse Wonder";
const count = (text) => Number(text.replace(/,/g, ""));

// The body without what the counts row shows, and without a curator posing as the author.
export function cardContext(item) {
  let preview = item.body || "";
  let author = item.author;
  let hn;
  const line = preview.match(HN_LINE);
  if (line) {
    hn = { points: count(line[1]), comments: count(line[2]), url: line[3] || "" };
    preview = preview.replace(HN_LINE, "");
  }
  if (item.source === "muse-wonder" || item.author === MUSE_NAME) {
    const kind = preview.match(MUSE_KIND);
    if (kind) preview = preview.slice(kind[0].length);
    preview = preview.replace(FOR_YOU, "");
    if (author === MUSE_NAME) author = "";
  }
  const out = { preview: preview.trim() };
  if (author !== undefined) out.author = author;
  if (hn) out.hn = hn;
  return out;
}

const YT_ID = /^[A-Za-z0-9_-]{11}$/;
function youTubeId(value) {
  if (!safeUrl(value)) return "";
  const url = new URL(value);
  const host = url.hostname.toLowerCase().replace(/^(www|m|music)\./, "");
  let id = "";
  if (host === "youtu.be") id = url.pathname.split("/")[1] || "";
  else if (host === "youtube.com" || host === "youtube-nocookie.com") {
    id = url.pathname === "/watch" ? url.searchParams.get("v") || "" : (/^\/(?:shorts|embed|live|v)\/([^/?#]+)/.exec(url.pathname) || [])[1] || "";
  }
  return YT_ID.test(id) ? id : "";
}

function vimeoId(value) {
  if (!safeUrl(value)) return "";
  const url = new URL(value);
  const host = url.hostname.toLowerCase().replace(/^www\./, "");
  if (host === "vimeo.com") return (/^\/(\d{5,})(?:\/|$)/.exec(url.pathname) || [])[1] || "";
  if (host === "player.vimeo.com") return (/^\/video\/(\d{5,})/.exec(url.pathname) || [])[1] || "";
  return "";
}

// t=90, t=1m30s, start=90.
function seconds(raw) {
  if (!raw) return 0;
  if (/^\d+$/.test(raw)) return Number(raw);
  const m = /^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$/.exec(raw);
  return m ? Number(m[1] || 0) * 3600 + Number(m[2] || 0) * 60 + Number(m[3] || 0) : 0;
}

const FILE = /\.(mp4|m4v|mov|webm|m3u8)(?:[?#]|$)/i;
const HLS = /\.m3u8(?:[?#]|$)/i;
const TIKTOK = /^https?:\/\/(?:www\.|m\.)?tiktok\.com\/(?:@[^/]+\/video|embed(?:\/v2)?|player\/v1)\/\d{15,21}(?:[/?#]|$)/i;
const REEL = /^https?:\/\/(?:www\.|m\.)?instagram\.com\/(?:[A-Za-z0-9._]+\/)?(?:reels?|tv)\/[A-Za-z0-9_-]{5,}(?:[/?#]|$)/i;

// The phone's videoSource. TikTok and Instagram have a kind but no player on this
// page: they are still videos, so a reader never fetches them as articles.
function videoSource(url, poster) {
  if (!url) return null;
  const yt = youTubeId(url);
  if (yt) {
    const params = new URL(url).searchParams;
    return { kind: "youtube", id: yt, start: seconds(params.get("t") || params.get("start")), poster, url };
  }
  const vimeo = vimeoId(url);
  if (vimeo) return { kind: "vimeo", id: vimeo, poster, url };
  if (TIKTOK.test(url)) return { kind: "tiktok", poster, url };
  if (REEL.test(url)) return { kind: "instagram", poster, url };
  if (FILE.test(url) || /^https:\/\/video\.twimg\.com\//.test(url)) return { kind: "file", url, poster };
  return null;
}

function mediaVideo(media) {
  if (!media || media.type === "image") return null;
  return videoSource(media.url, media.thumbnail_url) || { kind: "file", url: media.url, poster: media.thumbnail_url };
}

// The video a card plays in place of its picture: its own page when that is a
// video page, else its one video. A carousel of several keeps its pages.
export function cardVideo(item) {
  const media = item.media || [];
  const page = videoSource(item.url, media.find((m) => m.type === "image")?.url || item.image_url);
  if (page && page.kind !== "file" && media.filter((m) => m.type !== "image").length === 0 && media.length <= 1) return page;
  return media.length === 1 ? mediaVideo(media[0]) : null;
}

// What plays in the card on this page, or null to keep the poster that opens the
// link: an X post's lone video or the card's video. Desktop Chrome's <video> has no HLS.
export function cardPlayer(item) {
  const embed = item.embed && item.embed.type === "x_post" ? item.embed : null;
  const media = embed && embed.media;
  const source = embed ? (media && media.length === 1 ? mediaVideo(media[0]) : null) : cardVideo(item);
  if (!source) return null;
  if (source.kind === "youtube" || source.kind === "vimeo") return source;
  return source.kind === "file" && safeUrl(source.url) && !HLS.test(source.url) ? source : null;
}

// YouTube's and Vimeo's own players, starting at once.
export function playerUrl(source) {
  if (source.kind === "vimeo") return `https://player.vimeo.com/video/${source.id}?autoplay=1&playsinline=1`;
  return `https://www.youtube.com/embed/${source.id}?autoplay=1&playsinline=1&rel=0&modestbranding=1${source.start ? `&start=${source.start}` : ""}`;
}

// The page a card's reader fetches: its main url, unless the card is an X post or
// a video (those already show themselves).
export function articleUrl(item, isVideo) {
  if (!item.url || (item.embed && item.embed.type === "x_post") || isVideo) return null;
  return /^https?:\/\//i.test(item.url) ? item.url : null;
}

const LANDSCAPE = 1.91;
const PORTRAIT = 0.8;
const shapeOf = (entry) => (entry.width && entry.height ? Math.min(LANDSCAPE, Math.max(PORTRAIT, entry.width / entry.height)) : LANDSCAPE);

// An X post's photos, at most the four X lays out.
export function xPhotos(embed) {
  return (embed.media || []).filter((m) => m.type === "image" && safeUrl(m.url)).slice(0, 4);
}

// A video's box before and after it plays: 16:9 for a site's player, else its own shape.
export function videoAspect(source, shown) {
  return source.kind === "file" ? (shown ? shown.aspect : 1) : 16 / 9;
}

// The card's one picture and its box's shape, known before the image loads so
// a late image can never move a card (the phone's mediaAspect, in rich.ts). A
// video shows its poster.
export function picture(item) {
  const media = [...(item.media || []), ...(item.image_url ? [{ type: "image", url: item.image_url }] : []), ...((item.embed && item.embed.media) || [])];
  for (const entry of media) {
    const url = safeUrl(entry.type === "image" ? entry.url : entry.thumbnail_url);
    if (!url) continue;
    return { url, aspect: shapeOf(entry), alt: entry.alt || "" };
  }
  const youTube = youTubeId(item.url);
  return youTube ? { url: `https://i.ytimg.com/vi/${youTube}/hqdefault.jpg`, aspect: LANDSCAPE, alt: "" } : null;
}

// 45s, 12m, 3h, 4d, then Oct 5.
export function shortAge(iso, now = new Date()) {
  const date = iso ? new Date(iso) : null;
  if (!date || Number.isNaN(date.getTime())) return "";
  const s = Math.max(0, Math.floor((now.getTime() - date.getTime()) / 1000));
  if (s < 60) return s + "s";
  if (s < 3600) return Math.floor(s / 60) + "m";
  if (s < 86400) return Math.floor(s / 3600) + "h";
  if (s < 7 * 86400) return Math.floor(s / 86400) + "d";
  return date.toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });
}

const escapeHtml = (text) => String(text).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);

// A lone video file, in a frame only so that it plays with no referrer: X's video
// host refuses a foreign one, and <video> has no referrerpolicy of its own.
export function fileVideoDocument(source) {
  const poster = safeUrl(source.poster) ? ` poster="${escapeHtml(source.poster)}"` : "";
  return `<!doctype html><html><head><meta charset="utf-8"><meta name="referrer" content="no-referrer">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; img-src https: http:; media-src https: http:; style-src 'unsafe-inline'">
<style>html,body{margin:0;height:100%;background:#000;overflow:hidden}video{display:block;width:100%;height:100%}</style>
</head><body><video src="${escapeHtml(source.url)}"${poster} controls autoplay playsinline preload="auto"></video></body></html>`;
}

// The tallest article a frame will be sized to; anything over it is not a height.
const MAX_ARTICLE_HEIGHT = 2_000_000;

// What the reader's frame may say. Anything else, from anything else, is nothing.
export function parseArticleMessage(data, nonce) {
  if (!data || typeof data !== "object" || !nonce || data.nonce !== nonce) return null;
  if (data.type === "height" && Number.isFinite(data.height) && data.height > 0 && data.height <= MAX_ARTICLE_HEIGHT) return { type: "height", height: data.height };
  if (data.type === "link" && typeof data.url === "string" && safeUrl(data.url)) return { type: "link", url: data.url };
  if (data.type === "key" && data.key === "Escape") return { type: "key", key: "Escape" };
  return null;
}

// The reader's document: the phone's article.ts in this page's colors, sandboxed
// by feed.js with no same-origin, so nothing in it can reach the token in
// localStorage. Only this page's own script runs (a nonce'd CSP); the server
// already stripped the article's scripts, frames and handlers. The script speaks
// only by postMessage, each message carrying the nonce, which a page the frame
// navigated to would not know.
export function articleDocument(article, nonce) {
  const lead = article.image_url && !article.html.includes(article.image_url) ? `<figure><img src="${escapeHtml(article.image_url)}" alt=""></figure>` : "";
  return `<!doctype html><html${article.language ? ` lang="${escapeHtml(article.language)}"` : ""}><head>
<meta charset="utf-8">
<meta name="referrer" content="no-referrer">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; img-src https: http: data:; media-src https: http:; style-src 'unsafe-inline'; script-src 'nonce-${nonce}'; base-uri 'none'; form-action 'none'">
<style>
html, body { overflow: hidden; }
body { margin: 0; background: #0e1116; color: #d8dde5; font: 17px/1.6 system-ui, -apple-system, "Segoe UI", sans-serif; overflow-wrap: break-word; }
#root { padding: 1px 16px; }
p, ul, ol, blockquote, figure, pre, table { margin: 0 0 1em; }
h1, h2, h3, h4, h5, h6 { line-height: 1.25; margin: 1.3em 0 0.5em; }
h1 { font-size: 1.45em; } h2 { font-size: 1.3em; } h3 { font-size: 1.15em; } h4, h5, h6 { font-size: 1em; }
a { color: #e8b45a; text-decoration: none; }
a:hover { text-decoration: underline; }
img, video, picture { display: block; max-width: 100%; height: auto; border-radius: 10px; margin: 0 auto; }
figure { margin-left: 0; margin-right: 0; }
figcaption { color: #8b94a1; font-size: 0.8em; line-height: 1.4; margin-top: 6px; }
blockquote { border-left: 3px solid #2f3844; margin-left: 0; padding-left: 14px; color: #8b94a1; }
pre, code { font-family: ui-monospace, Menlo, monospace; font-size: 0.85em; }
pre { background: #161b23; padding: 12px; border-radius: 8px; overflow-x: auto; }
:not(pre) > code { background: #161b23; padding: 1px 4px; border-radius: 4px; }
table { display: block; overflow-x: auto; border-collapse: collapse; font-size: 0.9em; }
td, th { border: 1px solid #2f3844; padding: 6px 8px; }
hr { border: 0; border-top: 1px solid #2f3844; margin: 1.5em 0; }
</style></head><body><div id="root">${lead}${article.html}</div>
<script nonce="${nonce}">
(function () {
  var post = function (m) { m.nonce = ${JSON.stringify(nonce)}; parent.postMessage(m, "*"); };
  var onLink = function (e) {
    var a = e.target && e.target.closest ? e.target.closest("a[href]") : null;
    if (!a || (e.type === "auxclick" && e.button !== 1)) return;
    if ((a.getAttribute("href") || "").charAt(0) === "#") return;
    e.preventDefault();
    post({ type: "link", url: a.href });
  };
  document.addEventListener("click", onLink, true);
  document.addEventListener("auxclick", onLink, true);
  // A click in here takes the keyboard from the page.
  document.addEventListener("keydown", function (e) { if (e.key === "Escape") post({ type: "key", key: "Escape" }); });
  var root = document.getElementById("root");
  var last = 0;
  var report = function () {
    var h = Math.ceil(root.getBoundingClientRect().height);
    if (h !== last) { last = h; post({ type: "height", height: h }); }
  };
  if (window.ResizeObserver) new ResizeObserver(report).observe(root);
  document.querySelectorAll("img").forEach(function (img) { img.addEventListener("load", report); img.addEventListener("error", function () { img.style.display = "none"; report(); }); });
  window.addEventListener("load", report);
  report();
})();
</script></body></html>`;
}
