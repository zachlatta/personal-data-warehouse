// Sharing a card: the paper plane (feed) or the header button (detail view)
// opens the phone's share sheet with the card's title as the subject and its
// link and summary as the text, so it can go to Slack, Messages or a mail app
// (Zach, 2026-10-25: "use the native share thing"). Until then a mailto opened
// a draft in the default mail app only.

export type Shareable = { title?: string; url?: string; body?: string };

export type ShareDraft = { subject: string; body: string };

// Long enough to say what the link is, short enough to read as a note.
const SUMMARY_CHARS = 400;

function summaryOf(text: string): string {
  const flat = text.replace(/\s+/g, ' ').trim();
  if (flat.length <= SUMMARY_CHARS) return flat;
  const cut = flat.slice(0, SUMMARY_CHARS);
  const sentence = cut.lastIndexOf('. ');
  if (sentence > SUMMARY_CHARS / 2) return cut.slice(0, sentence + 1);
  return `${cut.slice(0, cut.lastIndexOf(' ')).trimEnd()}…`;
}

// preview is the card's body as plain text (markdown stripped).
export function shareDraft(item: Shareable, preview = ''): ShareDraft {
  const title = (item.title ?? '').trim();
  const url = (item.url ?? '').trim();
  const summary = preview ? summaryOf(preview) : '';
  const head = [title, url].filter(Boolean).join('\n');
  return { subject: title || url, body: [head, summary].filter(Boolean).join('\n\n') };
}
