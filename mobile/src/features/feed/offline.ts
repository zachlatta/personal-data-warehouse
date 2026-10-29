// The feed's offline copies on the phone (offline-store.ts is the plan and
// the bookkeeping). After every feed load online, prefetchOffline puts the
// saved queue's and the next cards' pictures in expo-image's disk cache and
// their articles, with up to 8 MB of pictures each, in Documents/pdw-offline.
// The detail view reads an article from here first; useOffline is whether
// the last request found no connection.
import { Directory, File, Paths } from 'expo-file-system';
import { Image } from 'expo-image';
import { useSyncExternalStore } from 'react';
import { AppState } from 'react-native';

import type { AppConfig } from '@/lib/config';

import { fetchArticle, FeedApiError, type FeedItem } from './api';
import { articleUrl, type FeedArticle } from './article';
import { recordUi } from './engagement';
import {
  cardMediaUrls,
  emptyIndex,
  evict,
  fileKey,
  ImageBudget,
  isOfflineError,
  localizeArticle,
  MAX_IMAGE_BYTES,
  parseIndex,
  pickArticleImages,
  planArticles,
  plannedCards,
  storeBytes,
  type OfflineIndex,
  type StoredImage,
} from './offline-store';
import { cardVideo } from './video';

// Whether the phone is offline: set by every feed request's outcome.
let offline = false;
const listeners = new Set<() => void>();

export function setOffline(value: boolean) {
  if (offline === value) return;
  offline = value;
  for (const listener of listeners) listener();
}

export function isOffline(): boolean {
  return offline;
}

export function useOffline(): boolean {
  return useSyncExternalStore(
    (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    () => offline,
  );
}

function dir(): Directory {
  const d = new Directory(Paths.document, 'pdw-offline');
  if (!d.exists) d.create({ intermediates: true });
  return d;
}

let index: OfflineIndex | null = null;

function loadIndex(): OfflineIndex {
  if (!index) {
    try {
      const f = new File(dir(), 'index.json');
      index = parseIndex(f.exists ? f.textSync() : null);
    } catch {
      index = emptyIndex();
    }
  }
  return index;
}

function saveIndex() {
  try {
    new File(dir(), 'index.json').write(JSON.stringify(loadIndex()));
  } catch {
    // The next run downloads again what the lost index forgot.
  }
}

function remove(name: string) {
  try {
    const f = new File(dir(), name);
    if (f.exists) f.delete();
  } catch {}
}

// A picture host (or a batch of them) that never answers must not hold up the rest.
const MEDIA_BATCH_MS = 20_000;

async function downloadImage(url: string, budget: ImageBudget): Promise<StoredImage | null> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), MEDIA_BATCH_MS);
  try {
    const response = await fetch(url, { signal: controller.signal });
    if (!response.ok) return null;
    const type = (response.headers.get('content-type') ?? '').split(';')[0].trim().toLowerCase();
    if (!type.startsWith('image/')) return null;
    if (Number(response.headers.get('content-length') ?? 0) > MAX_IMAGE_BYTES) return null;
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (!budget.admit(bytes.byteLength)) return null;
    const file = `i-${fileKey(url)}`;
    new File(dir(), file).write(bytes);
    return { file, bytes: bytes.byteLength, type };
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

// One article being stored at a time per url.
const storing = new Map<string, Promise<void>>();

/** Keep an article read online, with its pictures, for offline. */
export function storeArticle(url: string, article: FeedArticle): Promise<void> {
  const running = storing.get(url);
  if (running) return running;
  const job = (async () => {
    const idx = loadIndex();
    if (idx.entries[url]?.file) return;
    const file = `a-${fileKey(url)}.json`;
    const text = JSON.stringify(article);
    new File(dir(), file).write(text);
    const budget = new ImageBudget();
    const images: Record<string, StoredImage> = {};
    for (const src of pickArticleImages(article.html)) {
      const image = await downloadImage(src, budget);
      if (image) images[src] = image;
    }
    const now = Date.now();
    idx.entries[url] = { url, file, failed_at: 0, images, bytes: text.length + budget.used, saved_at: now, used_at: now };
    saveIndex();
  })()
    .catch(() => {})
    .finally(() => storing.delete(url));
  storing.set(url, job);
  return job;
}

function storeFailure(url: string) {
  const idx = loadIndex();
  const now = Date.now();
  idx.entries[url] = { url, file: '', failed_at: now, images: {}, bytes: 0, saved_at: now, used_at: now };
}

/**
 * The stored article at url with its pictures as data: URIs, or null when
 * it was not downloaded. The pictures are read asynchronously, before the
 * WebView is given the page.
 */
export async function readStoredArticle(url: string): Promise<FeedArticle | null> {
  const entry = loadIndex().entries[url];
  if (!entry?.file) return null;
  try {
    const f = new File(dir(), entry.file);
    if (!f.exists) return null;
    const article = JSON.parse(await f.text()) as FeedArticle;
    const uris: Record<string, string> = {};
    for (const [src, image] of Object.entries(entry.images)) {
      const img = new File(dir(), image.file);
      if (img.exists) uris[src] = `data:${image.type};base64,${await img.base64()}`;
    }
    entry.used_at = Date.now();
    saveIndex();
    return { ...article, html: localizeArticle(article.html, uris) };
  } catch {
    return null;
  }
}

/** Whether an article was downloaded. */
export function hasStoredArticle(url: string): boolean {
  return !!loadIndex().entries[url]?.file;
}

async function prefetchMedia(urls: string[]): Promise<void> {
  // prefetch resolves false at the first failure, so a few at a time.
  for (let i = 0; i < urls.length; i += 8) {
    if (AppState.currentState !== 'active') return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([
      Image.prefetch(urls.slice(i, i + 8), 'disk').catch(() => false),
      new Promise((resolve) => (timer = setTimeout(resolve, MEDIA_BATCH_MS))),
    ]);
    clearTimeout(timer);
  }
}

let running = false;

// An explicit save takes priority even while the ahead-of-reading batch is
// running. The reader uses the same storeArticle path and its per-url lock.
export async function prefetchSavedCard(config: AppConfig, item: FeedItem): Promise<void> {
  const url = articleUrl(item, !item.embed && !!cardVideo(item));
  const pictures = prefetchMedia(cardMediaUrls(item));
  try {
    if (url && !hasStoredArticle(url)) await storeArticle(url, await fetchArticle(config, url));
  } catch (error) {
    if (isOfflineError(error)) setOffline(true);
    // Saving is still valid when the article is unavailable. The reader
    // reports whether it has an offline copy, rather than claiming success.
  }
  await pictures;
}

/**
 * After a feed load online: download the saved queue's and the next cards'
 * pictures and articles, three at a time, until done, offline, or the app
 * leaves the front; then evict what is old or over the size cap.
 */
export async function prefetchOffline(config: AppConfig, items: FeedItem[], saved: FeedItem[]): Promise<void> {
  if (running) return;
  running = true;
  const startedAt = Date.now();
  const cards = plannedCards(saved, items);
  const media = [...new Set(cards.flatMap(cardMediaUrls))];
  const urls = cards.map((c) => articleUrl(c, !c.embed && !!cardVideo(c)));
  const todo = planArticles(urls, loadIndex(), startedAt);
  let articles = 0;
  let failed = 0;
  let stopped = false;
  try {
    const queue = [...todo];
    const worker = async () => {
      while (queue.length && !stopped) {
        if (AppState.currentState !== 'active') return;
        const url = queue.shift() as string;
        try {
          const article = await fetchArticle(config, url);
          await storeArticle(url, article);
          articles++;
        } catch (e) {
          if (isOfflineError(e)) {
            stopped = true;
            setOffline(true);
          } else {
            failed++;
            if (e instanceof FeedApiError) storeFailure(url);
          }
        }
      }
    };
    // The card pictures and the articles download side by side.
    await Promise.all([prefetchMedia(media), worker(), worker(), worker()]);
    const idx = loadIndex();
    const keep = new Set(urls.filter((u): u is string => !!u));
    const savedUrls = new Set(saved.map((c) => articleUrl(c, !c.embed && !!cardVideo(c))).filter((u): u is string => !!u));
    const gone = evict(idx, keep, savedUrls, Date.now()).map((url) => idx.entries[url]);
    for (const entry of gone) delete idx.entries[entry.url];
    // Two articles can share a picture file; it goes with the last of them.
    const inUse = new Set(Object.values(idx.entries).flatMap((e) => Object.values(e.images).map((i) => i.file)));
    for (const entry of gone) {
      if (entry.file) remove(entry.file);
      for (const image of Object.values(entry.images)) if (!inUse.has(image.file)) remove(image.file);
    }
    saveIndex();
    recordUi('offline_prefetch', {
      dwell_ms: Date.now() - startedAt,
      metadata: {
        cards: cards.length,
        media_urls: media.length,
        articles,
        failed,
        planned: todo.length,
        stopped_offline: stopped,
        stored: Object.values(idx.entries).filter((e) => e.file).length,
        store_bytes: storeBytes(idx),
      },
    });
  } finally {
    running = false;
  }
}
