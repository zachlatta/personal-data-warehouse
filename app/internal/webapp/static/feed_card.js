// What a Feed card says, as plain data. Ported from the phone
// (mobile/src/features/feed/: markdown.ts, links.ts, card-context.ts, rich.ts)
// because there is no build step here. feed.js turns it into text nodes and
// links only: nothing a card carries ever becomes markup.

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

const LANDSCAPE = 1.91;
const PORTRAIT = 0.8;

// The card's one picture and its box's shape, known before the image loads so
// a late image can never move a card (the phone's mediaAspect, in rich.ts). A
// video shows its poster.
export function picture(item) {
  const media = [...(item.media || []), ...(item.image_url ? [{ type: "image", url: item.image_url }] : []), ...((item.embed && item.embed.media) || [])];
  for (const entry of media) {
    const url = safeUrl(entry.type === "image" ? entry.url : entry.thumbnail_url);
    if (!url) continue;
    const aspect = entry.width && entry.height ? Math.min(LANDSCAPE, Math.max(PORTRAIT, entry.width / entry.height)) : LANDSCAPE;
    return { url, aspect, alt: entry.alt || "" };
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
