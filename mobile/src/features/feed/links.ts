// Every place a card points to, main link first. A card has its url (what a
// tap on the card opens) and up to ten more links (the paper, the repo, the
// discussion). index is what an open event records, so engagement can say
// which link he followed: 0 is the main url, 1.. the extra links in order.

export type FeedLink = { url: string; title?: string; site?: string };

export type CardLink = { url: string; label: string; site: string; index: number };

type Linked = { url?: string; site?: string; links?: FeedLink[] };

function hostOf(url: string): string {
  const match = /^https?:\/\/([^/?#]+)/i.exec(url);
  return match ? match[1].toLowerCase().replace(/^www\./, '') : '';
}

export function cardLinks(item: Linked): CardLink[] {
  const out: CardLink[] = [];
  if (item.url) out.push({ url: item.url, label: item.site || hostOf(item.url), site: item.site || hostOf(item.url), index: 0 });
  (item.links ?? []).forEach((link, i) => {
    if (!link.url) return;
    const site = link.site || hostOf(link.url);
    out.push({ url: link.url, label: link.title || site, site, index: i + 1 });
  });
  return out;
}
