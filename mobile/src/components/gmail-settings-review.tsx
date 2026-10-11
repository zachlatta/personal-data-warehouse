import { StyleSheet, View } from 'react-native';

import { StatusPill } from '@/components/status-pill';
import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import type { Mutation } from '@/lib/api';
import { formatWhen } from '@/lib/format';
import { gmailSettingsReview } from '@/lib/mutation-review';

// A Gmail filter or label change, read as the rule it is: "From x → skip
// inbox, label Newsletters", before → after for a replacement, and the recent
// mail the rule would have caught. The words come from the server so the web
// review and this one say the same thing.
export function GmailSettingsCard({ mutation, requestReason }: { mutation: Mutation; requestReason?: string }) {
  const theme = useTheme();
  const review = gmailSettingsReview(mutation);
  return (
    <View style={[styles.card, { backgroundColor: theme.backgroundElement }]}>
      <View style={styles.header}>
        <ThemedText type="smallBold" style={styles.headerTitle}>{review.heading}</ThemedText>
        <StatusPill status={mutation.status} />
      </View>
      <ThemedText type="small" themeColor="textSecondary">{mutation.account}</ThemedText>

      {review.kind === 'filter' ? (
        <View style={[styles.rule, { backgroundColor: theme.backgroundSelected }]}>
          {review.before ? (
            <>
              <ThemedText type="smallBold" style={styles.accent}>{review.after ? 'NOW' : 'RULE BEING REMOVED'}</ThemedText>
              <ThemedText type="small" style={review.after ? styles.before : undefined} selectable>{review.before}</ThemedText>
            </>
          ) : null}
          {review.after ? (
            <>
              <ThemedText type="smallBold" style={styles.accent}>{review.before ? 'BECOMES' : 'RULE'}</ThemedText>
              <ThemedText selectable>{review.after}</ThemedText>
            </>
          ) : null}
          {review.effect ? <ThemedText type="small" themeColor="textSecondary">{review.effect}</ThemedText> : null}
        </View>
      ) : (
        <View style={[styles.rule, { backgroundColor: theme.backgroundSelected }]}>
          {review.changes.map((change) => <ThemedText key={change} type="small">{change}</ThemedText>)}
          {review.newName && !review.changes.length ? <ThemedText type="small">New name: {review.newName}</ThemedText> : null}
          {review.color ? (
            <View style={[styles.chip, { backgroundColor: review.color.background }]}>
              <ThemedText type="smallBold" style={{ color: review.color.text }}>{review.newName || review.name}</ThemedText>
            </View>
          ) : null}
          {review.labelFacts ? <ThemedText type="small" themeColor="textSecondary">{review.labelFacts}</ThemedText> : null}
        </View>
      )}

      {review.warnings.map((warning) => <ThemedText key={warning} type="small" style={styles.warning}>{warning}</ThemedText>)}

      {review.kind === 'filter' && review.matchText ? (
        <View style={styles.match}>
          <ThemedText type="smallBold" themeColor="textSecondary">{review.matchText.toUpperCase()}</ThemedText>
          {review.matchNote ? <ThemedText type="small" themeColor="textSecondary">{review.matchNote}</ThemedText> : null}
          {review.examples.map((example) => (
            <View key={example.threadId} style={styles.example}>
              <View style={styles.exampleHeader}>
                <ThemedText type="smallBold" numberOfLines={1} style={styles.exampleSender}>{example.sender}</ThemedText>
                {example.date ? <ThemedText type="small" themeColor="textSecondary">{formatWhen(example.date)}</ThemedText> : null}
              </View>
              <ThemedText type="small" numberOfLines={1}>{example.subject}</ThemedText>
            </View>
          ))}
          {review.existingMailNote ? <ThemedText type="small" themeColor="textSecondary">{review.existingMailNote}</ThemedText> : null}
        </View>
      ) : null}

      {review.resultNote && (mutation.status === 'succeeded' || mutation.status === 'observed') ? <ThemedText type="small">{review.resultNote}</ThemedText> : null}
      {mutation.reason && mutation.reason !== requestReason ? <ThemedText type="small" themeColor="textSecondary">{mutation.reason}</ThemedText> : null}
      {mutation.error ? <ThemedText type="small" style={styles.warning}>{mutation.error}</ThemedText> : null}
    </View>
  );
}

const styles = StyleSheet.create({
  card: { marginHorizontal: Spacing.three, borderRadius: 12, padding: 12, gap: Spacing.two },
  header: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', gap: Spacing.two },
  headerTitle: { flex: 1 },
  rule: { borderRadius: 10, borderLeftWidth: 4, borderLeftColor: '#D97706', padding: 10, gap: Spacing.one },
  accent: { color: '#D97706', letterSpacing: 0.8, fontSize: 11 },
  before: { textDecorationLine: 'line-through', opacity: 0.7 },
  chip: { alignSelf: 'flex-start', borderRadius: 4, paddingHorizontal: 8, paddingVertical: 2 },
  warning: { color: '#D0342C' },
  match: { gap: Spacing.one },
  example: { borderRadius: 8, padding: 8, borderWidth: StyleSheet.hairlineWidth, borderColor: '#6B728044', gap: 2 },
  exampleHeader: { flexDirection: 'row', justifyContent: 'space-between', gap: Spacing.one },
  exampleSender: { flexShrink: 1 },
});
