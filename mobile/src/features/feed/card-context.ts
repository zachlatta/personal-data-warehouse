// What a card says about itself before its content, after the screen
// recordings of his X and Instagram feeds (2026-10-20): X puts a small line
// above a post ("Eugene Marinelli reposted"), Instagram one under the name
// ("Suggested for you"), and both show counts as a row, not as prose. The
// PDW recording showed the gaps: Hacker News cards whose only text was
// "80 points · 14 comments on Hacker News · discussion", and Muse cards
// that listed Muse as the author and opened with "*Essay* **For you:**".

import type { FeedEmbed } from './rich';

export type HnStats = { points: number; comments: number; url: string };

export type CardContext = {
  // The line above the card (the curator, the kind of piece, the interest
  // it was picked for); '' for none.
  label: string;
  // The body without what the label and counts now show (markdown).
  preview: string;
  // The author line, without a curator posing as the author.
  author?: string;
  hn?: HnStats;
};

type ContextInput = { source: string; body?: string; author?: string; metadata?: Record<string, unknown> };

const HN_LINE = /^\s*\**(\d[\d,]*) points\**\s*·\s*(\d[\d,]*) comments? on Hacker News\s*·\s*(?:\[discussion\]\(([^)\s]+)\)|discussion)\s*$/im;
const MUSE_KIND = /^\s*\*([^*\n]+)\*\s*(?:\n|$)/;
const FOR_YOU = /^\s*\*\*For you:\*\*\s*/i;
const MUSE_NAME = 'Muse Wonder';

function count(text: string): number {
  return Number(text.replace(/,/g, ''));
}

function text(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}

export function cardContext(item: ContextInput): CardContext {
  let preview = item.body ?? '';
  let label = '';
  let author = item.author;
  let hn: HnStats | undefined;

  const line = preview.match(HN_LINE);
  if (line) {
    hn = { points: count(line[1]), comments: count(line[2]), url: line[3] ?? '' };
    preview = preview.replace(HN_LINE, '');
  }

  if (item.source === 'muse-wonder' || item.author === MUSE_NAME) {
    const kind = preview.match(MUSE_KIND);
    if (kind) preview = preview.slice(kind[0].length);
    label = [MUSE_NAME, kind?.[1].trim()].filter(Boolean).join(' · ');
    preview = preview.replace(FOR_YOU, '');
    if (author === MUSE_NAME) author = '';
  } else {
    label = text(item.metadata?.taste);
  }

  const out: CardContext = { label, preview: preview.trim() };
  if (author !== undefined) out.author = author;
  if (hn) out.hn = hn;
  return out;
}

// Cards in this load that the last one did not have, for X's "new posts"
// pill. None on the first load: everything is new then.
export function newArrivals<T extends { item_id: string }>(previous: string[], next: T[]): T[] {
  if (!previous.length) return [];
  const seen = new Set(previous);
  return next.filter((item) => !seen.has(item.item_id));
}

// Where a tap on a post's author goes: their profile, as on X.
export function profileUrl(embed: Pick<FeedEmbed, 'type' | 'author_handle'>): string {
  return embed.author_handle ? `https://x.com/${embed.author_handle}` : '';
}
