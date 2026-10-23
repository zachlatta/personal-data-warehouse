// An email in a review is read, not inspected. Until 2026-10-11 the phone
// printed each message's text part, and for bulk mail that part is the HTML
// source — MSO conditionals, style blocks, analytics scripts, comment banners —
// or empty while the HTML part held the message. The review now renders the
// HTML part in a WebView the mail cannot script: its <script>s are removed,
// the page's Content-Security-Policy forbids any it smuggles in, and a tapped
// link opens outside the review. The only script that runs is the sizing one
// the app injects (a WKUserScript, which page CSP does not govern).

export type EmailBody = { kind: 'html'; html: string } | { kind: 'text'; text: string } | { kind: 'none' };

// Tall enough to read most of a message, short enough that a 40-thread batch
// is not a scroll through 40 newsletters; past it the body says "Show full email".
export const EMAIL_BODY_COLLAPSED_HEIGHT = 520;

const HTML_START = /^\s*(<!doctype\s+html|<html[\s>]|<head[\s>]|<body[\s>]|<!--|<meta[\s>]|<style[\s>]|<table[\s>]|<div[\s>])/i;
const HTML_TAG = /<\/?(html|head|body|div|table|tr|td|p|br|span|a|img|style|meta|font|center)\b[^>]*>/gi;

// Whether a text part is really HTML. A start tag or comment at the very top
// decides it; otherwise it takes a density of real tags, so "Use the <b> tag"
// in a plain-text note stays text.
export function looksLikeHtml(text: string): boolean {
  if (!text || !text.trim()) return false;
  if (HTML_START.test(text)) return true;
  const tags = text.match(HTML_TAG) ?? [];
  return tags.length >= 8;
}

export function emailBodyForDisplay(message: { bodyHtml?: string; text?: string; hasFullBody?: boolean }): EmailBody {
  const html = (message.bodyHtml ?? '').trim();
  if (html) return { kind: 'html', html };
  const text = message.text ?? '';
  if (looksLikeHtml(text)) return { kind: 'html', html: text };
  if (text.trim()) return { kind: 'text', text };
  return { kind: 'none' };
}

const REMOVED_ELEMENTS = ['script', 'iframe', 'frame', 'frameset', 'object', 'embed', 'applet', 'noscript', 'template'];

export function sanitizeEmailHtml(html: string): string {
  let out = html;
  for (const tag of REMOVED_ELEMENTS) {
    out = out.replace(new RegExp(`<${tag}\\b[\\s\\S]*?<\\/${tag}\\s*>`, 'gi'), '');
    out = out.replace(new RegExp(`<\\/?${tag}\\b[^>]*>`, 'gi'), '');
  }
  // A form posts a reader's typing somewhere; its fields are dropped with it.
  out = out.replace(/<form\b[\s\S]*?<\/form\s*>/gi, '').replace(/<\/?(form|input|button|select|textarea)\b[^>]*>/gi, '');
  // <base> would re-root every relative link; a refresh navigates on its own.
  out = out.replace(/<base\b[^>]*>/gi, '');
  out = out.replace(/<meta\b[^>]*http-equiv\s*=\s*["']?\s*refresh[^>]*>/gi, '');
  // Inline handlers (onload=, onclick=, onerror=...), quoted or not.
  out = out.replace(/\s+on[a-z]+\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)/gi, '');
  out = out.replace(/(href|src|action|formaction|xlink:href)\s*=\s*(["'])\s*javascript:[^"']*\2/gi, '$1=$2#$2');
  // App Transport Security blocks plain-http images in the WebView.
  out = out.replace(/(\ssrc\s*=\s*["'])http:\/\//gi, '$1https://');
  return out;
}

const POLICY = [
  "default-src 'none'",
  "script-src 'none'",
  "img-src https: data: cid:",
  "style-src 'unsafe-inline' https:",
  "font-src https: data:",
  "media-src https:",
].join('; ');

// Low-specificity defaults: the mail's own styles win wherever it sets them.
const BASE_STYLE = [
  'html{-webkit-text-size-adjust:100%}',
  'body{margin:0;padding:12px 14px;font:16px/1.45 -apple-system,system-ui,sans-serif;color:#111;background:#fff;word-wrap:break-word;overflow-wrap:anywhere}',
  'img{max-width:100%;height:auto}',
  'pre{white-space:pre-wrap}',
  'blockquote{margin-left:0;padding-left:10px;border-left:3px solid #ddd;color:#555}',
].join('');

const HEAD = `<meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="${POLICY}"><meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no"><style>${BASE_STYLE}</style>`;

// A whole document: the policy and viewport come first in the one <head>, so
// they govern everything the mail declares after them.
export function emailHtmlDocument(html: string): string {
  const clean = sanitizeEmailHtml(html).replace(/<!doctype[^>]*>/gi, '').trim();
  if (/<head\b[^>]*>/i.test(clean)) {
    const withHead = clean.replace(/<head\b[^>]*>/i, (open) => `${open}${HEAD}`);
    return /<html\b/i.test(withHead) ? `<!DOCTYPE html>${withHead}` : `<!DOCTYPE html><html>${withHead}</html>`;
  }
  if (/<html\b[^>]*>/i.test(clean)) {
    return `<!DOCTYPE html>${clean.replace(/<html\b[^>]*>/i, (open) => `${open}<head>${HEAD}</head>`)}`;
  }
  const body = /<body\b/i.test(clean) ? clean : `<body>${clean}</body>`;
  return `<!DOCTYPE html><html><head>${HEAD}</head>${body}</html>`;
}

// Bulk mail is laid out for a 600px column; the phone's is ~360px. The script
// scales an over-wide body down to fit (what Mail does) and reports the
// resulting height, again as images arrive, so the WebView is exactly as tall
// as the message and never scrolls inside the list.
export const EMAIL_SIZER_SCRIPT = `(function () {
  var zoomed = false, last = 0;
  function post() {
    var body = document.body, doc = document.documentElement;
    if (!body) return;
    if (!zoomed) {
      var width = Math.max(body.scrollWidth, doc.scrollWidth);
      var view = window.innerWidth || doc.clientWidth;
      if (view > 0 && width > view + 2) body.style.zoom = String(view / width);
      zoomed = true;
    }
    var height = Math.ceil(Math.max(doc.scrollHeight, body.getBoundingClientRect().height));
    if (height > 0 && Math.abs(height - last) > 1) {
      last = height;
      window.ReactNativeWebView.postMessage(JSON.stringify({ type: 'height', height: height }));
    }
  }
  post();
  window.addEventListener('load', post);
  var images = document.images || [];
  for (var i = 0; i < images.length; i++) images[i].addEventListener('load', post);
  setTimeout(post, 250);
  setTimeout(post, 1000);
  setTimeout(post, 3000);
})();
true;
`;

// What a navigation inside the email does: the document itself loads, a link
// the reader taps opens outside the review, and nothing else goes anywhere.
export function emailNavigation(url: string): 'load' | 'open' | 'block' {
  const value = url.trim().toLowerCase();
  if (value === 'about:blank' || value === 'about:srcdoc') return 'load';
  if (value.startsWith('https://') || value.startsWith('http://') || value.startsWith('mailto:') || value.startsWith('tel:')) return 'open';
  return 'block';
}

// Props the email WebView takes only on iOS. Android's WebView cannot parse
// dataDetectorTypes as a string and aborts the whole app on it.
export function emailWebViewPlatformProps(platform: string): { dataDetectorTypes?: 'none'; allowsLinkPreview?: boolean } {
  return platform === 'ios' ? { dataDetectorTypes: 'none', allowsLinkPreview: false } : {};
}
