// The event's own file (item_media) on the event screen. Images draw inline;
// a PDF (a scan, a finance document, a Drive PDF) gets a button that hands
// its signed link to the phone's PDF viewer.
import type { TimelineMedia } from './api';

// The server signs media links for an hour; a screen left open longer than
// this fetches the event again before opening, so the link never expires
// under the viewer.
export const MEDIA_LINK_FRESH_MS = 30 * 60 * 1000;

// The label for a media link the viewer opens, or null when the screen
// shows the media itself (images) or has nothing to open.
export function openMediaLabel(media: TimelineMedia | null | undefined): string | null {
  if (!media?.media_url) return null;
  return media.media_kind === 'pdf' ? 'Open PDF' : null;
}

export function mediaLinkIsStale(fetchedAt: number, now: number): boolean {
  return now - fetchedAt >= MEDIA_LINK_FRESH_MS;
}
