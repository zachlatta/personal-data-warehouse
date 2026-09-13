import { useState } from 'react';
import { Linking, Pressable, StyleSheet, View } from 'react-native';

import { Avatar } from '@/components/avatar';
import { StatusPill } from '@/components/status-pill';
import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import type { Mutation } from '@/lib/api';
import { pretty } from '@/lib/format';
import { contactMutationReview, type ContactFieldChange, type ContactOperationReview, type ContactReviewPoint } from '@/lib/mutation-review';

const CREATE = '#16A34A';
const UPDATE = '#2563EB';
const DELETE = '#DC2626';

function opColor(op: ContactOperationReview['op']): string {
  return op === 'create_contact' ? CREATE : op === 'update_contact' ? UPDATE : DELETE;
}

function pointHref(point: ContactReviewPoint): string {
  if (point.kind === 'email') return `mailto:${point.value}`;
  if (point.kind === 'phone') return `tel:${point.value.replace(/[^\d+]/g, '')}`;
  if (point.kind === 'url') return /^[a-z]+:\/\//i.test(point.value) ? point.value : `https://${point.value}`;
  return '';
}

const POINT_GLYPH: Record<ContactReviewPoint['kind'], string> = { email: '@', phone: '☏', url: '⌂', address: '⌖' };

// One way to reach the person. Tapping it opens the mail app, dialer or
// browser, because "is this the right email?" is often answered by looking
// at who it belongs to, not by the string itself.
function PointRow({ point }: { point: ContactReviewPoint }) {
  const theme = useTheme();
  const href = pointHref(point);
  const body = (
    <>
      <ThemedText type="smallBold" themeColor="textSecondary" style={styles.pointGlyph}>{POINT_GLYPH[point.kind]}</ThemedText>
      <ThemedText selectable style={styles.pointValue} numberOfLines={2}>{point.value}</ThemedText>
      {point.label ? <View style={[styles.typeChip, { borderColor: theme.backgroundSelected }]}><ThemedText style={styles.typeChipText}>{point.label.toUpperCase()}</ThemedText></View> : null}
    </>
  );
  if (!href) return <View style={styles.point}>{body}</View>;
  return (
    <Pressable
      accessibilityRole="link"
      accessibilityLabel={`Open ${point.value}`}
      onPress={() => { void Linking.openURL(href).catch(() => undefined); }}
      style={({ pressed }) => [styles.point, pressed && styles.pressed]}>
      {body}
    </Pressable>
  );
}

function ChangeRow({ change }: { change: ContactFieldChange }) {
  const theme = useTheme();
  return (
    <View style={[styles.change, { borderTopColor: theme.backgroundSelected }]}>
      <View style={styles.changeHeader}>
        <ThemedText type="smallBold">{change.label}</ThemedText>
        {change.kind === 'cleared' ? <ThemedText type="smallBold" style={styles.clearedTag}>CLEARED</ThemedText> : null}
        {change.kind === 'unchanged' ? <ThemedText type="small" themeColor="textSecondary">unchanged</ThemedText> : null}
      </View>
      {change.kind === 'unchanged' ? (
        <ThemedText type="small" selectable>{change.after || 'Not set'}</ThemedText>
      ) : (
        <>
          <ThemedText type="small" selectable style={styles.before}>{change.before || 'Not set'}</ThemedText>
          <ThemedText type="small" selectable style={change.kind === 'cleared' ? styles.clearedValue : styles.after}>
            {change.kind === 'cleared' ? 'Deleted' : change.after || 'Not set'}
          </ThemedText>
        </>
      )}
    </View>
  );
}

function ContactOperationView({ review, index, total }: { review: ContactOperationReview; index: number; total: number }) {
  const theme = useTheme();
  const [raw, setRaw] = useState(false);
  const color = opColor(review.op);
  return (
    <View style={[styles.operation, index > 0 && { borderTopWidth: StyleSheet.hairlineWidth, borderTopColor: theme.backgroundSelected }]}>
      <View style={styles.head}>
        <Avatar name={review.name} size={40} />
        <View style={styles.headCopy}>
          <View style={styles.eyebrow}>
            <ThemedText type="smallBold" style={[styles.verb, { color }]}>{review.verb.toUpperCase()}{total > 1 ? ` ${index + 1}/${total}` : ''}</ThemedText>
          </View>
          <ThemedText type="subtitle" style={styles.name}>{review.name}</ThemedText>
          {review.nickname ? <ThemedText type="small" themeColor="textSecondary">“{review.nickname}”</ThemedText> : null}
          {review.role ? <ThemedText type="small" themeColor="textSecondary">{review.role}</ThemedText> : null}
        </View>
      </View>

      {review.warning ? (
        <View style={styles.warning}><ThemedText type="small" style={styles.warningText}>{review.warning}</ThemedText></View>
      ) : null}

      {review.points.length ? (
        <View style={[styles.points, { backgroundColor: theme.background }]}>
          {review.points.map((point, i) => <PointRow key={`${point.kind}-${point.value}-${i}`} point={point} />)}
        </View>
      ) : null}

      {review.note ? (
        <View style={[styles.note, { borderLeftColor: color }]}>
          <ThemedText type="smallBold" themeColor="textSecondary" style={styles.noteTitle}>NOTE</ThemedText>
          <ThemedText type="small" selectable>{review.note}</ThemedText>
        </View>
      ) : null}

      {review.op === 'update_contact' ? (
        <View>
          <ThemedText type="small" themeColor="textSecondary">{review.effect} Fields not listed are untouched.</ThemedText>
          {review.changes.length ? review.changes.map((change) => <ChangeRow key={change.field} change={change} />) : (
            <ThemedText type="small" style={styles.warningText}>No update fields were provided.</ThemedText>
          )}
        </View>
      ) : (
        <ThemedText type="small" themeColor="textSecondary">{review.effect}</ThemedText>
      )}

      <Pressable accessibilityRole="button" accessibilityState={{ expanded: raw }} onPress={() => setRaw((value) => !value)} style={styles.rawToggle}>
        <ThemedText type="small" themeColor="textSecondary">{raw ? 'Hide' : 'Show'} raw operation{review.resourceName ? ` · ${review.resourceName}` : ''}</ThemedText>
      </Pressable>
      {raw ? <ThemedText type="small" selectable style={styles.raw}>{pretty(review.raw)}</ThemedText> : null}
    </View>
  );
}

export function ContactMutationCard({
  mutation,
  pending,
  onRemove,
  requestReason,
}: {
  mutation: Mutation;
  pending: boolean;
  onRemove: () => void;
  requestReason?: string;
}) {
  const theme = useTheme();
  const reviews = contactMutationReview(mutation);
  const removed = mutation.status === 'removed' || mutation.status === 'skipped' || mutation.status === 'rejected';
  const active = pending && !removed;
  return (
    <View style={[styles.card, { backgroundColor: theme.backgroundElement }, removed && styles.cardRemoved]}>
      {reviews.length === 0 ? (
        <View style={styles.operation}>
          <View style={styles.cardHeader}>
            <ThemedText type="smallBold">{mutation.operation}</ThemedText>
            <StatusPill status={mutation.status} />
          </View>
          <ThemedText type="small" selectable>{pretty(mutation.payload)}</ThemedText>
        </View>
      ) : reviews.map((review, index) => <ContactOperationView key={`${review.resourceName || review.name}-${index}`} review={review} index={index} total={reviews.length} />)}
      {mutation.status !== 'pending_review' ? (
        <View style={[styles.footer, { borderTopColor: theme.backgroundSelected }]}><StatusPill status={mutation.status} /></View>
      ) : null}
      {mutation.reason && mutation.reason !== requestReason ? (
        <ThemedText type="small" themeColor="textSecondary" style={styles.reason}>{mutation.reason}</ThemedText>
      ) : null}
      {mutation.error ? <ThemedText style={[styles.reason, styles.warningText]}>{mutation.error}</ThemedText> : null}
      {active ? (
        <Pressable accessibilityRole="button" onPress={onRemove} style={[styles.skip, { borderTopColor: theme.backgroundSelected }]}>
          <ThemedText type="smallBold" style={styles.skipText}>Skip this contact</ThemedText>
        </Pressable>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  card: { borderRadius: 12, overflow: 'hidden' },
  cardRemoved: { opacity: 0.5 },
  cardHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', gap: Spacing.two },
  operation: { padding: Spacing.three, gap: Spacing.two },
  head: { flexDirection: 'row', gap: 12, alignItems: 'flex-start' },
  headCopy: { flex: 1, minWidth: 0, gap: 2 },
  eyebrow: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two },
  verb: { letterSpacing: 0.8, fontSize: 11 },
  name: { fontSize: 19, lineHeight: 24 },
  warning: { borderRadius: 10, borderLeftWidth: 4, borderLeftColor: DELETE, backgroundColor: '#DC26261A', padding: 10 },
  warningText: { color: '#D0342C' },
  points: { borderRadius: 10, overflow: 'hidden' },
  point: { minHeight: 40, flexDirection: 'row', alignItems: 'center', gap: 10, paddingHorizontal: 12, paddingVertical: 8 },
  pressed: { opacity: 0.6 },
  pointGlyph: { width: 16, textAlign: 'center' },
  pointValue: { flex: 1, minWidth: 0 },
  typeChip: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 4, paddingHorizontal: 4, paddingVertical: 1 },
  typeChipText: { color: '#8B8F98', fontSize: 9, fontWeight: '700', letterSpacing: 0.5 },
  note: { borderLeftWidth: 3, paddingLeft: 10, gap: 2 },
  noteTitle: { letterSpacing: 0.8, fontSize: 11 },
  change: { paddingTop: Spacing.two, marginTop: Spacing.two, borderTopWidth: StyleSheet.hairlineWidth, gap: 2 },
  changeHeader: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', gap: Spacing.two },
  before: { color: '#8B8F98', textDecorationLine: 'line-through' },
  after: { color: CREATE },
  clearedTag: { color: DELETE, letterSpacing: 0.8, fontSize: 11 },
  clearedValue: { color: DELETE, fontStyle: 'italic' },
  rawToggle: { paddingTop: Spacing.one },
  raw: { fontFamily: 'Menlo', fontSize: 11, lineHeight: 15 },
  footer: { paddingHorizontal: Spacing.three, paddingVertical: Spacing.two, borderTopWidth: StyleSheet.hairlineWidth },
  reason: { paddingHorizontal: Spacing.three, paddingBottom: Spacing.two },
  skip: { minHeight: 44, alignItems: 'center', justifyContent: 'center', borderTopWidth: StyleSheet.hairlineWidth },
  skipText: { color: DELETE },
});
