import { useEffect, useMemo, useRef, useState } from 'react';
import { StyleSheet, TextInput, View } from 'react-native';

import { Avatar } from '@/components/avatar';
import { ComposeMeta } from '@/components/compose-meta';
import { ThemedText } from '@/components/themed-text';
import { Palette, Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import type { Mutation, UpdateTextMessageMutationInput } from '@/lib/api';
import { formatWhen } from '@/lib/format';
import { textMessageReview } from '@/lib/mutation-review';

const TEXT_MAX = 4000;

// A text from the Pixel, reviewed as the message it is: who receives it, the
// latest messages with them, and the words. The words are editable while the
// request is pending and save through update-text-message, so the edit is on
// the server before approval: approval sends what is stored, never what is on
// screen. The number is not editable here or on the web: a wrong number is a
// deny.
export function TextMessageCard({
  mutation,
  pending: requestPending = mutation.status === 'pending_review',
  busy = false,
  onSave,
  onPendingChange,
  requestReason,
  locked,
  alone,
}: {
  mutation: Mutation;
  pending?: boolean;
  busy?: boolean;
  onSave?: (input: UpdateTextMessageMutationInput) => Promise<void>;
  // Unsaved text is reported up: Send approves the stored message, so the
  // screen saves this on the way to sending.
  onPendingChange?: (input: UpdateTextMessageMutationInput | null) => void;
  requestReason?: string;
  // The screen's scroll lock: not focusable while the page moves.
  locked?: boolean;
  // The request's only message: drawn on the page itself, not as a card on
  // it, so the draft gets the screen's full width.
  alone?: boolean;
}) {
  const theme = useTheme();
  // What sits on the card, or on the page when there is no card.
  const surface = alone ? theme.backgroundElement : theme.background;
  const review = useMemo(() => textMessageReview(mutation), [mutation]);
  const pending = requestPending && mutation.status === 'pending_review';
  const editable = pending && !!onSave;
  const [text, setText] = useState(review.text);
  const [error, setError] = useState('');
  const [saved, setSaved] = useState(false);
  // Reset before painting a new server baseline, rather than rendering stale
  // text and synchronously setting state from an effect.
  const [baseline, setBaseline] = useState(review.text);
  if (baseline !== review.text) {
    setBaseline(review.text);
    setText(review.text);
    setSaved(false);
  }
  const dirty = text.trim() !== review.text.trim();
  const [focused, setFocused] = useState(false);
  // Held in a ref: the parent passes a fresh closure every render.
  const report = useRef(onPendingChange);
  useEffect(() => {
    report.current = onPendingChange;
  }, [onPendingChange]);
  const unsavedText = editable && dirty && text.trim() ? text.trim() : '';
  useEffect(() => {
    report.current?.(unsavedText ? { text: unsavedText } : null);
  }, [unsavedText]);
  useEffect(() => {
    const reporter = report;
    return () => reporter.current?.(null);
  }, []);
  const save = async () => {
    if (!onSave) return;
    const trimmed = text.trim();
    if (!trimmed) { setError('The message cannot be empty.'); return; }
    if (trimmed.length > TEXT_MAX) { setError(`Texts are at most ${TEXT_MAX} characters.`); return; }
    setError('');
    try {
      await onSave({ text: trimmed });
      setSaved(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };
  return (
    <View style={[styles.card, alone ? styles.cardAlone : { backgroundColor: theme.backgroundElement }]}>
      <View style={styles.header}>
        <Avatar name={review.recipientName || review.recipient} size={30} />
        <View style={styles.headerCopy}>
          <ThemedText type="smallBold" numberOfLines={1}>{review.recipientName || review.recipient || 'Unknown number'}</ThemedText>
          <ThemedText type="small" themeColor="textSecondary" numberOfLines={1}>
            {review.recipientName ? `${review.recipient} · ` : ''}text from your Pixel
          </ThemedText>
        </View>
      </View>

      {alone ? (
        <ThemedText type="small" themeColor="textSecondary">{review.effect}</ThemedText>
      ) : (
        <View style={[styles.action, { backgroundColor: theme.backgroundSelected }]}>
          <ThemedText type="smallBold" style={styles.accent}>WHAT WILL HAPPEN</ThemedText>
          <ThemedText type="small">{review.effect}</ThemedText>
        </View>
      )}
      {review.warnings.map((warning) => (
        <View key={warning} style={styles.warning}><ThemedText type="small" style={styles.error}>{warning}</ThemedText></View>
      ))}

      {review.recent.length ? (
        <View style={styles.context}>
          <ThemedText type="smallBold" themeColor="textSecondary">LATEST MESSAGES</ThemedText>
          {review.recent.map((message, index) => (
            <View key={`${message.at}-${index}`} style={[styles.bubbleRow, message.isFromMe && styles.bubbleRowMine]}>
              <View style={[styles.bubble, { backgroundColor: message.isFromMe ? theme.backgroundSelected : surface }]}>
                <ThemedText type="small">{message.text}</ThemedText>
                <ThemedText type="small" themeColor="textSecondary" style={styles.when}>{message.actor} · {formatWhen(message.at)}</ThemedText>
              </View>
            </View>
          ))}
        </View>
      ) : null}

      <View style={styles.compose}>
        <ThemedText type="smallBold" themeColor="textSecondary">
          {review.sent ? 'MESSAGE SENT' : 'YOUR MESSAGE'}{review.edited ? ' · EDITED IN REVIEW' : ''}
        </ThemedText>
        {editable ? (
          <>
            <TextInput
              accessibilityLabel="Message text"
              value={text}
              onChangeText={(value) => { setText(value); setSaved(false); }}
              editable={!busy && (focused || !locked)}
              onFocus={() => setFocused(true)}
              onBlur={() => setFocused(false)}
              multiline
              autoCapitalize="sentences"
              autoCorrect
              maxLength={TEXT_MAX}
              style={[styles.input, { color: theme.text, backgroundColor: surface, borderColor: dirty ? Palette.warning : '#8A878444' }]}
            />
            <ComposeMeta
              hint={saved && !dirty ? 'Saved. Send sends this text.' : dirty ? 'Edited. Send saves it first.' : 'Tap to edit before sending.'}
              length={text.length}
              max={TEXT_MAX}
              dirty={dirty}
              busy={busy}
              onSave={() => { void save(); }}
            />
            {error ? <ThemedText type="small" style={styles.error}>{error}</ThemedText> : null}
          </>
        ) : (
          <View style={[styles.bubble, styles.finalBubble, { backgroundColor: surface }]}>
            <ThemedText selectable>{review.text || '(no text)'}</ThemedText>
          </View>
        )}
        {review.sent ? (
          <ThemedText type="small" themeColor="textSecondary">
            {review.sent.alreadySent ? 'Already sent earlier' : 'Sent'}{review.sent.protocol ? ` over ${review.sent.protocol}` : ''}{review.sent.state ? ` (${review.sent.state})` : ''}
          </ThemedText>
        ) : null}
      </View>
      {mutation.reason && mutation.reason !== requestReason ? <ThemedText type="small" themeColor="textSecondary">{mutation.reason}</ThemedText> : null}
      {mutation.error ? <ThemedText style={styles.error}>{mutation.error}</ThemedText> : null}
    </View>
  );
}

const styles = StyleSheet.create({
  card: { borderRadius: 12, padding: 10, gap: Spacing.two },
  cardAlone: { padding: 0, gap: Spacing.three },
  header: { flexDirection: 'row', alignItems: 'center', gap: 9 },
  headerCopy: { flex: 1, minWidth: 0, gap: 2 },
  accent: { color: Palette.warning, letterSpacing: 0.8 },
  action: { borderRadius: 10, borderLeftWidth: 4, borderLeftColor: Palette.warning, padding: 10, gap: Spacing.one },
  warning: { borderWidth: 1, borderColor: '#E5636D66', borderRadius: 10, padding: 10 },
  context: { gap: Spacing.one },
  bubbleRow: { flexDirection: 'row', justifyContent: 'flex-start' },
  bubbleRowMine: { justifyContent: 'flex-end' },
  bubble: { maxWidth: '85%', borderRadius: 12, paddingHorizontal: 10, paddingVertical: 7, borderWidth: StyleSheet.hairlineWidth, borderColor: '#8A878444', gap: 2 },
  finalBubble: { maxWidth: '100%' },
  when: { fontSize: 11 },
  compose: { gap: Spacing.one },
  input: { minHeight: 110, borderRadius: 10, padding: 12, borderWidth: 1, fontSize: 16, lineHeight: 22, textAlignVertical: 'top' },
  error: { color: Palette.danger },
});
