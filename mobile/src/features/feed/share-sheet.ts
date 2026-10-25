import { Alert, Share } from 'react-native';

import type { FeedItem } from './api';
import { recordFeedEvent, recordUi } from './engagement';
import { shareDraft } from './share';

// Opens the phone's share sheet for a card. A share event records where it
// was shared from and, on iOS, the app picked (Android does not say); a sheet
// closed without picking an app records no share.
export async function shareCard(item: FeedItem, preview: string, ctx: { position?: number; via: string }) {
  const draft = shareDraft(item, preview);
  let result;
  try {
    result = await Share.share({ title: draft.subject, message: draft.body }, { subject: draft.subject, dialogTitle: 'Share' });
  } catch (e) {
    Alert.alert('Could not share this card', e instanceof Error ? e.message : undefined);
    recordUi('share_failed', { item_id: item.item_id, position: ctx.position, metadata: { in: ctx.via } });
    return;
  }
  if (result.action !== Share.sharedAction) return;
  recordFeedEvent('share', {
    item_id: item.item_id,
    position: ctx.position,
    metadata: { via: 'share_sheet', in: ctx.via, url: item.url ?? '', ...(result.activityType ? { app: result.activityType } : {}) },
  });
}
