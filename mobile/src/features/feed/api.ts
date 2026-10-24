import type { AppConfig } from '@/lib/config';

import type { FeedArticle } from './article';
import type { FeedEvent } from './event-queue';
import type { FeedLink } from './links';
import type { FeedEmbed, FeedMedia } from './rich';

export type FeedItem = {
  item_id: string;
  source: string;
  key: string;
  title: string;
  body?: string;
  url?: string;
  links?: FeedLink[];
  image_url?: string;
  media?: FeedMedia[];
  embed?: FeedEmbed;
  author?: string;
  site?: string;
  tags: string[];
  score: number;
  published_at?: string;
  created_at: string;
  // Whatever the pushing agent kept (why it was picked, taste, hn_points).
  metadata?: Record<string, unknown>;
  engagement: { impressions: number; opens: number; engaged: boolean; engagement_score: number; saved?: boolean; liked?: boolean };
  // A seen card shown again after every unseen one, so the feed never runs dry.
  resurfaced?: boolean;
};

// saved is his read-later queue, newest saved first (absent on an older server).
export type FeedPage = { items: FeedItem[]; saved?: FeedItem[]; generated_at: string };

export class FeedApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function feedRequest<T>(config: AppConfig, path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set('Authorization', `Bearer ${config.clientName}:${config.token}`);
  headers.set('Accept', 'application/json');
  headers.set('X-PDW-Client', config.clientName);
  if (init.body) headers.set('Content-Type', 'application/json');
  const response = await fetch(`${config.baseUrl}${path}`, { ...init, headers });
  const text = await response.text();
  if (!response.ok) {
    let message = text.trim() || `HTTP ${response.status}`;
    try {
      const parsed = JSON.parse(text);
      if (parsed && typeof parsed.error === 'string') message = parsed.error;
    } catch {}
    throw new FeedApiError(response.status, message);
  }
  return (text ? JSON.parse(text) : null) as T;
}

export function fetchFeed(config: AppConfig, limit = 200): Promise<FeedPage> {
  return feedRequest<FeedPage>(config, `/api/feed?limit=${limit}`);
}

// His feed history: every card he was shown, opened, saved or liked that
// has every word of q, title matches first, then newest seen. An empty q is
// the whole history.
export function searchFeed(config: AppConfig, q: string, limit = 100): Promise<{ query: string; items: FeedItem[] }> {
  return feedRequest(config, `/api/feed/search?q=${encodeURIComponent(q)}&limit=${limit}`);
}

export async function postFeedEvents(config: AppConfig, device: string, events: FeedEvent[]): Promise<void> {
  await feedRequest(config, '/api/feed/events', { method: 'POST', body: JSON.stringify({ device, events }) });
}

// The readable article at a card's url, for its detail view. A page with no
// article answers 422 (FeedApiError).
export function fetchArticle(config: AppConfig, url: string): Promise<FeedArticle> {
  return feedRequest<FeedArticle>(config, `/api/feed/article?url=${encodeURIComponent(url)}`);
}
