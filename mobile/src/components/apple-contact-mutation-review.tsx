import { useState } from 'react';
import { Pressable, StyleSheet, View } from 'react-native';

import { Avatar } from '@/components/avatar';
import { ChangeRow, CONTACT_CREATE, CONTACT_DELETE, CONTACT_UPDATE, PointRow } from '@/components/contact-mutation-review';
import { StatusPill } from '@/components/status-pill';
import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import type { Mutation } from '@/lib/api';
import { pretty } from '@/lib/format';
import { appleContactPointExists, appleContactsReview, type AppleContactCard, type AppleContactsReview } from '@/lib/mutation-review';

const MERGE = '#7C3AED';

function opColor(op: AppleContactsReview['op']): string {
  return op === 'create' ? CONTACT_CREATE : op === 'update' ? CONTACT_UPDATE : MERGE;
}

// One card as it exists today. In a merge the kept card is framed in the
// merge colour and every other card says it will be deleted, because the
// question a reviewer is answering is "are these really the same person?".
function CurrentCard({ card, merge }: { card: AppleContactCard; merge: boolean }) {
  const theme = useTheme();
  const accent = card.kept ? MERGE : merge ? CONTACT_DELETE : theme.backgroundSelected;
  return (
    <View style={[styles.currentCard, { backgroundColor: theme.background, borderLeftColor: accent }]}>
      <View style={styles.currentHead}>
        <Avatar name={card.name || card.cardId} size={30} />
        <View style={styles.currentCopy}>
          <ThemedText type="smallBold" numberOfLines={1}>{card.missing ? 'Card no longer in the address book' : card.name || 'No name'}</ThemedText>
          {card.role ? <ThemedText type="small" themeColor="textSecondary" numberOfLines={1}>{card.role}</ThemedText> : null}
        </View>
        {merge ? (
          <ThemedText type="smallBold" style={[styles.cardTag, { color: card.kept ? MERGE : CONTACT_DELETE }]}>{card.kept ? 'KEPT' : 'DELETED'}</ThemedText>
        ) : null}
      </View>
      {card.points.length ? <View>{card.points.map((point, i) => <PointRow key={`${point.kind}-${point.value}-${i}`} point={point} />)}</View> : null}
      {card.note ? <ThemedText type="small" themeColor="textSecondary" numberOfLines={3} style={styles.currentNote}>{card.note}</ThemedText> : null}
      {!card.missing && !card.points.length && !card.note ? <ThemedText type="small" themeColor="textSecondary" style={styles.currentNote}>Only a name.</ThemedText> : null}
    </View>
  );
}

export function AppleContactMutationCard({
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
  const [raw, setRaw] = useState(false);
  const review = appleContactsReview(mutation);
  const color = opColor(review.op);
  const removed = mutation.status === 'removed' || mutation.status === 'skipped' || mutation.status === 'rejected';
  const active = pending && !removed;
  const merge = review.op === 'merge';
  return (
    <View style={[styles.card, { backgroundColor: theme.backgroundElement }, removed && styles.cardRemoved]}>
      <View style={styles.operation}>
        <View style={styles.head}>
          <Avatar name={review.name} size={40} />
          <View style={styles.headCopy}>
            <ThemedText type="smallBold" style={[styles.verb, { color }]}>{review.verb.toUpperCase()} · APPLE CONTACTS</ThemedText>
            <ThemedText type="subtitle" style={styles.name}>{review.name}</ThemedText>
            {review.nickname ? <ThemedText type="small" themeColor="textSecondary">“{review.nickname}”</ThemedText> : null}
            {review.role ? <ThemedText type="small" themeColor="textSecondary">{review.role}</ThemedText> : null}
          </View>
        </View>

        {review.warning ? (
          <View style={styles.warning}><ThemedText type="small" style={styles.warningText}>{review.warning}</ThemedText></View>
        ) : null}
        {review.destructive.length ? (
          <View style={[styles.warning, styles.destructive]}>
            {review.destructive.map((line) => <ThemedText key={line} type="small" style={styles.destructiveText}>• {line[0].toUpperCase()}{line.slice(1)}</ThemedText>)}
          </View>
        ) : null}

        {review.points.length ? (
          <View>
            <ThemedText type="smallBold" themeColor="textSecondary" style={styles.sectionTitle}>{review.op === 'create' ? 'HOW TO REACH' : 'ADDED'}</ThemedText>
            <View style={[styles.points, { backgroundColor: theme.background }]}>
              {review.points.map((point, i) => {
                const exists = review.op !== 'create' && appleContactPointExists(point, review.cards);
                return <PointRow key={`${point.kind}-${point.value}-${i}`} point={point} tag={exists ? 'already there' : undefined} />;
              })}
            </View>
          </View>
        ) : null}

        {review.removed.length ? (
          <View>
            <ThemedText type="smallBold" style={[styles.sectionTitle, { color: CONTACT_DELETE }]}>REMOVED</ThemedText>
            <View style={[styles.points, { backgroundColor: theme.background }]}>
              {review.removed.map((point, i) => <PointRow key={`rm-${point.kind}-${point.value}-${i}`} point={point} muted />)}
            </View>
          </View>
        ) : null}

        {review.changes.length ? (
          <View>
            <ThemedText type="smallBold" themeColor="textSecondary" style={styles.sectionTitle}>{merge ? 'SET ON THE KEPT CARD' : 'FIELDS'}</ThemedText>
            {review.changes.map((change) => <ChangeRow key={change.field} change={change} />)}
          </View>
        ) : null}

        {review.note || review.appendNote ? (
          <View style={[styles.note, { borderLeftColor: color }]}>
            <ThemedText type="smallBold" themeColor="textSecondary" style={styles.noteTitle}>{review.appendNote ? 'ADDED TO NOTE' : review.op === 'create' ? 'NOTE' : 'NOTE (REPLACES)'}</ThemedText>
            <ThemedText type="small" selectable>{review.appendNote || review.note}</ThemedText>
          </View>
        ) : null}

        <ThemedText type="small" themeColor="textSecondary">{review.effect}</ThemedText>

        {review.cards.length ? (
          <View style={styles.current}>
            <ThemedText type="smallBold" themeColor="textSecondary" style={styles.sectionTitle}>{merge ? `CARDS BEING MERGED · ${review.cards.length}` : 'CARD TODAY'}</ThemedText>
            {review.cards.map((card) => <CurrentCard key={card.cardId} card={card} merge={merge} />)}
          </View>
        ) : null}

        <Pressable accessibilityRole="button" accessibilityState={{ expanded: raw }} onPress={() => setRaw((value) => !value)} style={styles.rawToggle}>
          <ThemedText type="small" themeColor="textSecondary">{raw ? 'Hide' : 'Show'} raw operation</ThemedText>
        </Pressable>
        {raw ? <ThemedText type="small" selectable style={styles.raw}>{pretty(review.raw)}</ThemedText> : null}
      </View>
      {mutation.status !== 'pending_review' ? (
        <View style={[styles.footer, { borderTopColor: theme.backgroundSelected }]}><StatusPill status={mutation.status} /></View>
      ) : null}
      {mutation.reason && mutation.reason !== requestReason ? (
        <ThemedText type="small" themeColor="textSecondary" style={styles.reason}>{mutation.reason}</ThemedText>
      ) : null}
      {mutation.error ? <ThemedText style={[styles.reason, styles.warningText]}>{mutation.error}</ThemedText> : null}
      {active ? (
        <Pressable accessibilityRole="button" onPress={onRemove} style={[styles.skip, { borderTopColor: theme.backgroundSelected }]}>
          <ThemedText type="smallBold" style={styles.skipText}>{merge ? 'Skip this merge' : 'Skip this contact'}</ThemedText>
        </Pressable>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  card: { borderRadius: 12, overflow: 'hidden' },
  cardRemoved: { opacity: 0.5 },
  operation: { padding: Spacing.three, gap: Spacing.two },
  head: { flexDirection: 'row', gap: 12, alignItems: 'flex-start' },
  headCopy: { flex: 1, minWidth: 0, gap: 2 },
  verb: { letterSpacing: 0.8, fontSize: 11 },
  name: { fontSize: 19, lineHeight: 24 },
  sectionTitle: { letterSpacing: 0.8, fontSize: 11, marginBottom: 4 },
  warning: { borderRadius: 10, borderLeftWidth: 4, borderLeftColor: CONTACT_DELETE, backgroundColor: '#DC26261A', padding: 10, gap: 2 },
  warningText: { color: '#D0342C' },
  destructive: { borderLeftColor: '#D97706', backgroundColor: '#D977061A' },
  destructiveText: { color: '#B45309' },
  points: { borderRadius: 10, overflow: 'hidden' },
  note: { borderLeftWidth: 3, paddingLeft: 10, gap: 2 },
  noteTitle: { letterSpacing: 0.8, fontSize: 11 },
  current: { gap: Spacing.two },
  currentCard: { borderRadius: 10, borderLeftWidth: 4, padding: 10, gap: 6 },
  currentHead: { flexDirection: 'row', alignItems: 'center', gap: 10 },
  currentCopy: { flex: 1, minWidth: 0 },
  currentNote: { paddingHorizontal: 12 },
  cardTag: { letterSpacing: 0.8, fontSize: 10 },
  rawToggle: { paddingTop: Spacing.one },
  raw: { fontFamily: 'Menlo', fontSize: 11, lineHeight: 15 },
  footer: { paddingHorizontal: Spacing.three, paddingVertical: Spacing.two, borderTopWidth: StyleSheet.hairlineWidth },
  reason: { paddingHorizontal: Spacing.three, paddingBottom: Spacing.two },
  skip: { minHeight: 44, alignItems: 'center', justifyContent: 'center', borderTopWidth: StyleSheet.hairlineWidth },
  skipText: { color: CONTACT_DELETE },
});
