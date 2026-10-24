// The whole article inline in a card's detail view (Zach's voice memo,
// 2026-10-24: "embed the entire article content directly inline instead of
// me having to tap out"). The server reads the page (GET /api/feed/article);
// this builds the page the detail view's WebView shows: the article in the
// app's colors and type, every link handed back to the app.

export type FeedArticle = {
  url: string;
  title?: string;
  byline?: string;
  site?: string;
  image_url?: string;
  published_at?: string;
  language?: string;
  html: string;
  words: number;
};

type ArticleCard = {
  url?: string;
  embed?: { type?: string } | null;
};

// The page a card's detail view reads inline: its main url, unless the card
// is an X post or a video (those already show themselves).
export function articleUrl(item: ArticleCard, isVideo: boolean): string | null {
  if (!item.url || item.embed?.type === 'x_post' || isVideo) return null;
  return /^https?:\/\//i.test(item.url) ? item.url : null;
}

// About 230 words a minute, never under a minute.
export function readingMinutes(words: number): number {
  return Math.max(1, Math.round(words / 230));
}

// The last path segment without a WordPress-style -1024x683 size suffix or
// an extension, so a resized copy of the card's photo matches the original.
export function imageKey(raw: string): string {
  try {
    const name = new URL(raw).pathname.split('/').filter(Boolean).pop() ?? '';
    return name.replace(/\.[a-z0-9]+$/i, '').replace(/-\d+x\d+$/, '').toLowerCase();
  } catch {
    return '';
  }
}

export type ArticleColors = { text: string; secondary: string; background: string; element: string; rule: string; link: string };

export type ArticleMessage = { type: 'height'; height: number } | { type: 'link'; url: string };

export function parseArticleMessage(data: string): ArticleMessage | null {
  try {
    const m = JSON.parse(data);
    if (m?.type === 'height' && typeof m.height === 'number' && m.height > 0) return { type: 'height', height: m.height };
    if (m?.type === 'link' && typeof m.url === 'string' && /^(https?|mailto):/i.test(m.url)) return { type: 'link', url: m.url };
  } catch {}
  return null;
}

function escapeHtml(s: string): string {
  return s.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c] as string);
}

/**
 * The document the WebView shows. shownImages are the photos the detail
 * view already shows above it: the article's copy of the first one is
 * hidden. When the card has no photo, the article's lead image goes on top.
 * Only this page's own script runs (a nonce'd CSP); the server already
 * stripped the article's scripts, frames and handlers.
 */
export function articleDocument(article: FeedArticle, colors: ArticleColors, shownImages: string[], nonce: string): string {
  const shown = shownImages.map(imageKey).filter(Boolean);
  const lead =
    !shown.length && article.image_url && !article.html.includes(article.image_url)
      ? `<figure><img src="${escapeHtml(article.image_url)}" alt=""></figure>`
      : '';
  const c = colors;
  return `<!doctype html><html${article.language ? ` lang="${escapeHtml(article.language)}"` : ''}><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; img-src https: http: data:; media-src https: http:; style-src 'unsafe-inline'; script-src 'nonce-${nonce}'">
<style>
html { -webkit-text-size-adjust: 100%; }
body { margin: 0; background: ${c.background}; color: ${c.text}; font: 17px/1.6 -apple-system, system-ui, Roboto, sans-serif; overflow-wrap: break-word; }
#root { padding: 1px 16px; }
p, ul, ol, blockquote, figure, pre, table { margin: 0 0 1em; }
h1, h2, h3, h4, h5, h6 { line-height: 1.25; margin: 1.3em 0 0.5em; }
h1 { font-size: 1.45em; } h2 { font-size: 1.3em; } h3 { font-size: 1.15em; } h4, h5, h6 { font-size: 1em; }
a { color: ${c.link}; text-decoration: none; }
img, video, picture { display: block; max-width: 100%; height: auto; border-radius: 10px; margin: 0 auto; }
figure { margin-left: 0; margin-right: 0; }
figcaption { color: ${c.secondary}; font-size: 0.8em; line-height: 1.4; margin-top: 6px; }
blockquote { border-left: 3px solid ${c.rule}; margin-left: 0; padding-left: 14px; color: ${c.secondary}; }
pre, code { font-family: ui-monospace, Menlo, monospace; font-size: 0.85em; }
pre { background: ${c.element}; padding: 12px; border-radius: 8px; overflow-x: auto; }
:not(pre) > code { background: ${c.element}; padding: 1px 4px; border-radius: 4px; }
table { display: block; overflow-x: auto; border-collapse: collapse; font-size: 0.9em; }
td, th { border: 1px solid ${c.rule}; padding: 6px 8px; }
hr { border: 0; border-top: 1px solid ${c.rule}; margin: 1.5em 0; }
</style></head><body><div id="root">${lead}${article.html}</div>
<script nonce="${nonce}">
(function () {
  var post = function (m) { window.ReactNativeWebView && window.ReactNativeWebView.postMessage(JSON.stringify(m)); };
  var key = function (u) {
    try { var n = new URL(u, location.href).pathname.split('/').filter(Boolean).pop() || ''; return n.replace(/\\.[a-z0-9]+$/i, '').replace(/-\\d+x\\d+$/, '').toLowerCase(); } catch (e) { return ''; }
  };
  var shown = ${JSON.stringify(shown)};
  if (shown.length) {
    var imgs = document.querySelectorAll('#root img');
    for (var i = 0; i < imgs.length; i++) {
      var img = imgs[i];
      var keys = [img.getAttribute('src') || ''].concat((img.getAttribute('srcset') || '').split(',').map(function (s) { return s.trim().split(' ')[0]; }));
      if (keys.some(function (k) { return k && shown.indexOf(key(k)) >= 0; })) {
        var box = img.closest('figure') || img;
        box.parentNode.removeChild(box);
        break;
      }
    }
  }
  document.addEventListener('click', function (e) {
    var a = e.target && e.target.closest ? e.target.closest('a[href]') : null;
    if (!a) return;
    var href = a.getAttribute('href') || '';
    if (href.charAt(0) === '#') return;
    e.preventDefault();
    post({ type: 'link', url: a.href });
  }, true);
  var root = document.getElementById('root');
  var last = 0;
  var report = function () {
    var h = Math.ceil(root.getBoundingClientRect().height);
    if (h !== last) { last = h; post({ type: 'height', height: h }); }
  };
  if (window.ResizeObserver) new ResizeObserver(report).observe(root);
  document.querySelectorAll('img').forEach(function (img) { img.addEventListener('load', report); img.addEventListener('error', function () { img.style.display = 'none'; report(); }); });
  window.addEventListener('load', report);
  report();
})();
</script></body></html>`;
}
