// Just enough markdown for a card body: blocks for the reader, plain text
// for the card preview. Agents write light markdown (paragraphs, bullets,
// headings, bold, links); nothing here needs a full parser.

export type Span = { text: string; bold?: boolean; italic?: boolean; code?: boolean; url?: string };
export type Block =
  | { kind: 'heading'; level: number; spans: Span[] }
  | { kind: 'paragraph'; spans: Span[] }
  | { kind: 'bullet'; spans: Span[]; ordered?: string }
  | { kind: 'quote'; spans: Span[] }
  | { kind: 'code'; text: string };

const INLINE = /(\*\*([^*]+)\*\*|__([^_]+)__|\*([^*\s][^*]*)\*|_([^_\s][^_]*)_|`([^`]+)`|\[([^\]]+)\]\(([^)\s]+)\))/g;

export function parseInline(text: string): Span[] {
  const spans: Span[] = [];
  let last = 0;
  for (const match of text.matchAll(INLINE)) {
    const index = match.index ?? 0;
    if (index > last) spans.push({ text: text.slice(last, index) });
    if (match[2] ?? match[3]) spans.push({ text: (match[2] ?? match[3]) as string, bold: true });
    else if (match[4] ?? match[5]) spans.push({ text: (match[4] ?? match[5]) as string, italic: true });
    else if (match[6]) spans.push({ text: match[6], code: true });
    else if (match[7]) spans.push({ text: match[7], url: match[8] });
    last = index + match[0].length;
  }
  if (last < text.length) spans.push({ text: text.slice(last) });
  return spans;
}

export function parseMarkdown(markdown: string): Block[] {
  const blocks: Block[] = [];
  const lines = markdown.replace(/\r\n?/g, '\n').split('\n');
  let paragraph: string[] = [];
  const endParagraph = () => {
    if (paragraph.length) blocks.push({ kind: 'paragraph', spans: parseInline(paragraph.join(' ')) });
    paragraph = [];
  };
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const trimmed = line.trim();
    if (trimmed.startsWith('```')) {
      endParagraph();
      const code: string[] = [];
      for (i++; i < lines.length && !lines[i].trim().startsWith('```'); i++) code.push(lines[i]);
      blocks.push({ kind: 'code', text: code.join('\n') });
      continue;
    }
    if (trimmed === '') {
      endParagraph();
      continue;
    }
    const heading = /^(#{1,6})\s+(.*)$/.exec(trimmed);
    const bullet = /^[-*+]\s+(.*)$/.exec(trimmed);
    const ordered = /^(\d+)[.)]\s+(.*)$/.exec(trimmed);
    if (heading) {
      endParagraph();
      blocks.push({ kind: 'heading', level: heading[1].length, spans: parseInline(heading[2]) });
    } else if (bullet) {
      endParagraph();
      blocks.push({ kind: 'bullet', spans: parseInline(bullet[1]) });
    } else if (ordered) {
      endParagraph();
      blocks.push({ kind: 'bullet', spans: parseInline(ordered[2]), ordered: ordered[1] });
    } else if (trimmed.startsWith('>')) {
      endParagraph();
      blocks.push({ kind: 'quote', spans: parseInline(trimmed.replace(/^>\s?/, '')) });
    } else {
      paragraph.push(trimmed);
    }
  }
  endParagraph();
  return blocks;
}

/** The body as one line of plain text, for the card preview. */
export function plainText(markdown: string): string {
  return parseMarkdown(markdown)
    .map((block) => (block.kind === 'code' ? block.text : block.spans.map((s) => s.text).join('')))
    .join(' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// A card's preview as one run of text that keeps its links (blue, tappable,
// as Muse's feed shows them) and bold, with blocks joined by spaces.
export function previewSpans(markdown: string): Span[] {
  const out: Span[] = [];
  for (const block of parseMarkdown(markdown)) {
    if (out.length) out.push({ text: ' ' });
    if (block.kind === 'code') out.push({ text: block.text });
    else for (const span of block.spans) out.push(span.url ? { text: span.text, url: span.url } : span.bold ? { text: span.text, bold: true } : { text: span.text });
  }
  return out;
}
