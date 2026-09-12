import type { NotificationEvent } from './api';

// One word per notification for the ledger row. "Delivered" is the push
// service accepting it, which is not the same as it appearing on screen, and
// no observed open does not mean it was ignored (the tab says both).
export type NotificationOutcome = { label: string; tone: 'good' | 'neutral' | 'muted' | 'bad' };

export function notificationOutcome(event: NotificationEvent): NotificationOutcome {
  if (event.opened > 0) return { label: 'opened', tone: 'good' };
  if (event.accepted > 0) return { label: 'delivered', tone: 'neutral' };
  if (event.suppressed_replied > 0) return { label: 'skipped: already replied', tone: 'muted' };
  if (event.suppressed_read > 0) return { label: 'skipped: already read', tone: 'muted' };
  if (event.failed > 0) return { label: 'failed', tone: 'bad' };
  if (event.status === 'no_devices') return { label: 'no devices', tone: 'bad' };
  if (event.status === 'cancelled') return { label: 'cancelled', tone: 'muted' };
  return { label: 'sending', tone: 'neutral' };
}

export type NotificationSummary = {
  total: number;
  delivered: number;
  opened: number;
  skipped: number;
  failed: number;
  // Opened over delivered; null when nothing was delivered.
  openRate: number | null;
};

export function summarizeNotifications(events: NotificationEvent[]): NotificationSummary {
  const summary: NotificationSummary = { total: events.length, delivered: 0, opened: 0, skipped: 0, failed: 0, openRate: null };
  for (const event of events) {
    const { label } = notificationOutcome(event);
    if (label === 'opened') {
      summary.opened += 1;
      summary.delivered += 1;
    } else if (label === 'delivered') summary.delivered += 1;
    else if (label.startsWith('skipped')) summary.skipped += 1;
    else if (label === 'failed' || label === 'no devices') summary.failed += 1;
  }
  if (summary.delivered > 0) summary.openRate = summary.opened / summary.delivered;
  return summary;
}
