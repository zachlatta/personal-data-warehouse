// Sharing a card by email: a swipe opens a new draft in the phone's default
// mail app (Superhuman when it is the default) with the card's title as the
// subject and its link and summary as the body, and he picks who it goes to.
// A mailto is the only form a mail app fills in: Superhuman's mobile app opens
// superhuman:///compose/<mailto> and its universal link as an empty draft
// (2026-10-20), although its web router reads the mailto's fields.

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

export function mailtoUrl(draft: ShareDraft): string {
  return `mailto:?subject=${encodeURIComponent(draft.subject)}&body=${encodeURIComponent(draft.body)}`;
}
