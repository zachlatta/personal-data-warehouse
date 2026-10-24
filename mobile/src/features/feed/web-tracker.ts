// What the in-app browser sees of a page (Zach, 2026-10-24: "It'd be better if
// it measured everything I was doing inside the open web view, so that way it
// can see if I'm actually interacting and what I'm actually doing").
//
// TRACKER_JS runs inside every page the feed's browser loads and posts small
// JSON messages to the app: the page itself (title, description, length), a
// throttled "still here" on any touch, scroll or key, where he scrolled to and
// the paragraph and heading in the middle of the screen, taps (and the link
// or button text), selected and copied text, and media playing. It never reads
// what he types: a field focus reports only the field's type.
//
// PageVisit (pure, tested by web-tracker.test.mjs) folds those messages into
// one summary per page; tracked-browser.tsx records them as `web` events.

export const TRACKER_JS = `(function () {
  if (window.__pdwTracker) return true;
  window.__pdwTracker = true;
  var send = function (m) {
    try { window.ReactNativeWebView.postMessage(JSON.stringify(m)); } catch (e) {}
  };
  var clip = function (s, n) {
    s = String(s == null ? '' : s).replace(/\\s+/g, ' ').trim();
    return s.length > n ? s.slice(0, n - 1) + '\\u2026' : s;
  };
  var meta = function (sel) {
    var el = document.querySelector(sel);
    return el ? clip(el.getAttribute('content'), 300) : '';
  };
  var words = function () {
    var root = document.querySelector('article') || document.querySelector('main') || document.body;
    var t = root ? root.innerText || '' : '';
    var m = t.match(/\\S+/g);
    return m ? m.length : 0;
  };
  var height = function () {
    return Math.max(document.documentElement.scrollHeight, document.body ? document.body.scrollHeight : 0);
  };
  var page = function () {
    send({
      k: 'page',
      url: location.href,
      title: clip(document.title, 300),
      description: meta('meta[property="og:description"]') || meta('meta[name="description"]'),
      site_name: meta('meta[property="og:site_name"]'),
      words: words(),
      height: height(),
      viewport: window.innerHeight,
      text: centerText(),
    });
  };
  var lastAct = 0;
  var act = function () {
    var now = Date.now();
    if (now - lastAct < 2000) return;
    lastAct = now;
    send({ k: 'act' });
  };
  var BLOCK = /^(P|LI|H[1-6]|BLOCKQUOTE|PRE|TD|TH|FIGCAPTION|DD|DT|ARTICLE|SECTION)$/;
  var centerText = function () {
    var el = document.elementFromPoint(window.innerWidth / 2, window.innerHeight / 2);
    for (var i = 0; el && i < 6 && !BLOCK.test(el.tagName); i++) el = el.parentElement;
    return el ? clip(el.innerText, 200) : '';
  };
  var heading = function () {
    var hs = document.querySelectorAll('h1, h2, h3');
    var best = '';
    for (var i = 0; i < hs.length; i++) {
      if (hs[i].getBoundingClientRect().top < window.innerHeight / 2) best = hs[i].innerText;
      else break;
    }
    return clip(best, 120);
  };
  var settle = null;
  window.addEventListener('scroll', function () {
    act();
    clearTimeout(settle);
    settle = setTimeout(function () {
      var h = height() - window.innerHeight;
      send({
        k: 'scroll',
        y: Math.round(window.scrollY),
        pct: h > 0 ? Math.min(100, Math.round((window.scrollY / h) * 100)) : 100,
        text: centerText(),
        heading: heading(),
      });
    }, 400);
  }, { passive: true, capture: true });
  ['touchstart', 'keydown', 'wheel'].forEach(function (t) {
    window.addEventListener(t, act, { passive: true, capture: true });
  });
  document.addEventListener('click', function (e) {
    var el = e.target;
    var link = el && el.closest ? el.closest('a[href]') : null;
    var button = el && el.closest ? el.closest('button, [role="button"], summary, label') : null;
    var target = link || button || el;
    send({
      k: 'tap',
      tag: target && target.tagName ? target.tagName.toLowerCase() : '',
      text: target ? clip(target.innerText || target.getAttribute('aria-label') || target.getAttribute('alt') || '', 160) : '',
      href: link ? link.href : '',
    });
  }, true);
  document.addEventListener('focusin', function (e) {
    var el = e.target;
    if (!el || !/^(INPUT|TEXTAREA|SELECT)$/.test(el.tagName)) return;
    send({ k: 'field', type: el.type || el.tagName.toLowerCase() });
  }, true);
  var selTimer = null;
  document.addEventListener('selectionchange', function () {
    clearTimeout(selTimer);
    selTimer = setTimeout(function () {
      var a = document.activeElement;
      if (a && /^(INPUT|TEXTAREA)$/.test(a.tagName)) return;
      var s = String(window.getSelection ? window.getSelection() : '').trim();
      if (s.length >= 3) send({ k: 'select', text: clip(s, 500) });
    }, 1200);
  });
  document.addEventListener('copy', function () {
    var a = document.activeElement;
    if (a && /^(INPUT|TEXTAREA)$/.test(a.tagName)) return;
    send({ k: 'copy', text: clip(String(window.getSelection ? window.getSelection() : ''), 500) });
  }, true);
  ['play', 'pause', 'ended'].forEach(function (t) {
    document.addEventListener(t, function (e) {
      var m = e.target;
      if (!m || !/^(VIDEO|AUDIO)$/.test(m.tagName)) return;
      send({
        k: 'media',
        action: t,
        kind: m.tagName.toLowerCase(),
        src: clip(m.currentSrc || m.src || '', 300),
        t: Math.round(m.currentTime || 0),
        duration: isFinite(m.duration) ? Math.round(m.duration) : null,
      });
    }, true);
  });
  // Pages that change their address without loading (single-page apps).
  var lastUrl = location.href.split('#')[0];
  var check = function () {
    var u = location.href.split('#')[0];
    if (u === lastUrl) return;
    lastUrl = u;
    setTimeout(page, 300);
  };
  ['pushState', 'replaceState'].forEach(function (f) {
    var orig = history[f];
    history[f] = function () { var r = orig.apply(this, arguments); check(); return r; };
  });
  window.addEventListener('popstate', check);
  if (document.readyState === 'complete') page();
  else window.addEventListener('load', page);
  return true;
})();`;

export type TrackerMessage =
  | { k: 'page'; url: string; title?: string; description?: string; site_name?: string; words?: number; height?: number; viewport?: number; text?: string }
  | { k: 'act' }
  | { k: 'scroll'; y: number; pct: number; text?: string; heading?: string }
  | { k: 'tap'; tag?: string; text?: string; href?: string }
  | { k: 'field'; type?: string }
  | { k: 'select'; text: string }
  | { k: 'copy'; text?: string }
  | { k: 'media'; action: 'play' | 'pause' | 'ended'; kind?: string; src?: string; t?: number; duration?: number | null };

const KINDS = new Set(['page', 'act', 'scroll', 'tap', 'field', 'select', 'copy', 'media']);

/** The message a page posted, or null for anything else (a page's own postMessage). */
export function parseTrackerMessage(data: string): TrackerMessage | null {
  try {
    const m = JSON.parse(data) as { k?: unknown };
    return m && typeof m === 'object' && typeof m.k === 'string' && KINDS.has(m.k) ? (m as TrackerMessage) : null;
  } catch {
    return null;
  }
}

/** Same page: the address without its #fragment. */
export function samePage(a: string, b: string): boolean {
  return a.split('#')[0] === b.split('#')[0];
}

// Where he stopped scrolling and what was in the middle of the screen, kept
// with the active time until his next stop: what he actually read.
const MAX_READ = 20;
const MAX_HEADINGS = 30;
const MIN_READ_MS = 2000;

export type PageSummary = {
  url: string;
  title: string;
  description: string;
  site_name: string;
  words: number;
  /** Order of this page in the browser sitting, from 0. */
  page_index: number;
  from_url: string;
  max_scroll_pct: number;
  scrolls: number;
  taps: number;
  links_tapped: number;
  selections: number;
  copies: number;
  fields: number;
  media_plays: number;
  /** Headings he scrolled past, in order. */
  headings: string[];
  /** The paragraphs he stopped on, with the active time on each (longest first). */
  read: { text: string; ms: number }[];
};

export class PageVisit {
  url: string;
  /** The page has reported itself (a 'page' message came). */
  loaded = false;
  private info: Partial<PageSummary> = {};
  private maxPct = 0;
  private scrolls = 0;
  private taps = 0;
  private linksTapped = 0;
  private selections = 0;
  private copies = 0;
  private fields = 0;
  private plays = 0;
  private headings: string[] = [];
  private read = new Map<string, number>();
  private stop: { text: string; since: number } | null = null;

  readonly index: number;
  readonly fromUrl: string;

  constructor(url: string, index: number, fromUrl: string) {
    this.url = url;
    this.index = index;
    this.fromUrl = fromUrl;
  }

  /** Folds one message in. activeMs is the active clock's reading now. */
  add(m: TrackerMessage, activeMs: number) {
    switch (m.k) {
      case 'page':
        // A redirect or the page's own address: the one it reports wins.
        if (m.url) this.url = m.url;
        this.loaded = true;
        if (!this.stop && m.text) this.stop = { text: m.text, since: activeMs };
        this.info = {
          title: m.title ?? '',
          description: m.description ?? '',
          site_name: m.site_name ?? '',
          words: m.words ?? 0,
        };
        break;
      case 'scroll':
        this.scrolls += 1;
        this.maxPct = Math.max(this.maxPct, m.pct);
        if (m.heading && !this.headings.includes(m.heading) && this.headings.length < MAX_HEADINGS) this.headings.push(m.heading);
        this.settle(activeMs);
        this.stop = m.text ? { text: m.text, since: activeMs } : null;
        break;
      case 'tap':
        this.taps += 1;
        if (m.href) this.linksTapped += 1;
        break;
      case 'select':
        this.selections += 1;
        break;
      case 'copy':
        this.copies += 1;
        break;
      case 'field':
        this.fields += 1;
        break;
      case 'media':
        if (m.action === 'play') this.plays += 1;
        break;
      case 'act':
        break;
    }
  }

  private settle(activeMs: number) {
    if (!this.stop) return;
    const ms = Math.max(0, activeMs - this.stop.since);
    this.read.set(this.stop.text, (this.read.get(this.stop.text) ?? 0) + ms);
    this.stop = null;
  }

  /** The page's summary when he leaves it. */
  finish(activeMs: number): PageSummary {
    this.settle(activeMs);
    const read = [...this.read.entries()]
      .filter(([, ms]) => ms >= MIN_READ_MS)
      .sort((a, b) => b[1] - a[1])
      .slice(0, MAX_READ)
      .map(([text, ms]) => ({ text, ms: Math.round(ms) }));
    return {
      url: this.url,
      title: this.info.title ?? '',
      description: this.info.description ?? '',
      site_name: this.info.site_name ?? '',
      words: this.info.words ?? 0,
      page_index: this.index,
      from_url: this.fromUrl,
      max_scroll_pct: this.maxPct,
      scrolls: this.scrolls,
      taps: this.taps,
      links_tapped: this.linksTapped,
      selections: this.selections,
      copies: this.copies,
      fields: this.fields,
      media_plays: this.plays,
      headings: this.headings,
      read,
    };
  }
}
