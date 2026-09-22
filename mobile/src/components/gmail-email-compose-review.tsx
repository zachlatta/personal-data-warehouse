import { useMemo, useState } from 'react';
import { Pressable, StyleSheet, TextInput, View } from 'react-native';

import { Avatar } from '@/components/avatar';
import { StatusPill } from '@/components/status-pill';
import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import type { Mutation, UpdateEmailMutationInput } from '@/lib/api';
import { cleanSnippet, formatWhen } from '@/lib/format';
import {
  gmailEmailEditsFor,
  gmailEmailReview,
  gmailEmailUpdateInput,
  type GmailEmailEdits,
  type GmailEmailReplyThread,
  type GmailEmailVariant,
} from '@/lib/mutation-review';

const ACCENT = '#2563EB';
const DANGER = '#DC2626';

type DeliveryMode = 'send' | 'draft';

function sameEdits(a: GmailEmailEdits, b: GmailEmailEdits): boolean {
  return a.to === b.to && a.cc === b.cc && a.bcc === b.bcc && a.subject === b.subject && a.editorText === b.editorText;
}

function ReplyThread({ thread, account }: { thread: GmailEmailReplyThread; account: string }) {
  const theme = useTheme();
  const [open, setOpen] = useState(false);
  return (
    <View style={[styles.thread, { borderColor: theme.backgroundSelected }]}>
      <Pressable accessibilityRole="button" accessibilityState={{ expanded: open }} onPress={() => setOpen((value) => !value)} style={styles.threadHead}>
        <View style={styles.threadCopy}>
          <ThemedText type="small" themeColor="textSecondary">REPLYING IN THREAD</ThemedText>
          <ThemedText type="smallBold" numberOfLines={open ? undefined : 1}>{thread.subject || '(no subject)'}</ThemedText>
          <ThemedText type="small" themeColor="textSecondary">{thread.messages.length} message{thread.messages.length === 1 ? '' : 's'}</ThemedText>
        </View>
        <ThemedText themeColor="textSecondary" style={[styles.chevron, open && styles.chevronOpen]}>›</ThemedText>
      </Pressable>
      {open
        ? thread.messages.map((message, index) => {
            const body = message.hasFullBody ? message.text : cleanSnippet(message.text ?? '').trim();
            return (
              <View key={message.messageId || index} style={[styles.threadMessage, { borderTopColor: theme.backgroundSelected }]}>
                <View style={styles.threadMessageHead}>
                  <Avatar name={message.senderName} size={28} />
                  <View style={styles.threadCopy}>
                    <ThemedText type="smallBold">{message.senderName}</ThemedText>
                    <ThemedText type="small" themeColor="textSecondary" numberOfLines={1}>to {message.to.join(', ') || account}</ThemedText>
                  </View>
                  <ThemedText type="small" themeColor="textSecondary">{formatWhen(message.sentAt)}</ThemedText>
                </View>
                {body ? <ThemedText selectable style={styles.threadBody}>{body}</ThemedText> : null}
                {!message.hasFullBody ? <ThemedText type="small" themeColor="textSecondary">Only a preview is available.</ThemedText> : null}
              </View>
            );
          })
        : null}
    </View>
  );
}

function Field({ label, value, onChange, editable, autoCapitalize, keyboardType }: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  editable: boolean;
  autoCapitalize?: 'none' | 'sentences';
  keyboardType?: 'default' | 'email-address';
}) {
  const theme = useTheme();
  return (
    <View style={[styles.fieldRow, { borderBottomColor: theme.backgroundSelected }]}>
      <ThemedText type="small" themeColor="textSecondary" style={styles.fieldLabel}>{label}</ThemedText>
      <TextInput
        accessibilityLabel={label}
        value={value}
        onChangeText={onChange}
        editable={editable}
        autoCapitalize={autoCapitalize ?? 'none'}
        autoCorrect={autoCapitalize === 'sentences'}
        keyboardType={keyboardType ?? 'default'}
        style={[styles.fieldInput, { color: theme.text }]}
      />
    </View>
  );
}

// One gmail.send_email mutation, rendered as the composer it is: recipients,
// subject and the body are editable, the signature and quoted thread sit
// below read-only exactly as they will be sent, and a proposal with several
// variants is chosen from here. Saving posts to update-email, so the edit is
// on the server before the request is approved — approval sends what is
// stored, never what is on screen.
export function GmailEmailComposeCard({
  mutation,
  pending,
  busy,
  onSave,
  onRemove,
  requestReason,
}: {
  mutation: Mutation;
  pending: boolean;
  busy: boolean;
  onSave: (input: UpdateEmailMutationInput) => Promise<void>;
  onRemove: () => void;
  requestReason?: string;
}) {
  const theme = useTheme();
  const review = useMemo(() => gmailEmailReview(mutation), [mutation]);
  const [selectedId, setSelectedId] = useState(review.selectedVariantId);
  const [deliveryMode, setDeliveryMode] = useState<DeliveryMode>(review.deliveryMode);
  const [edits, setEdits] = useState<Record<string, GmailEmailEdits>>(() => Object.fromEntries(review.variants.map((variant) => [variant.id, gmailEmailEditsFor(variant)])));
  const [quotedOpen, setQuotedOpen] = useState(false);
  const [error, setError] = useState('');
  const [saved, setSaved] = useState(false);

  const variant: GmailEmailVariant = review.variants.find((item) => item.id === selectedId) ?? review.variants[0];
  const current = edits[variant.id] ?? gmailEmailEditsFor(variant);
  const removed = mutation.status === 'removed' || mutation.status === 'skipped' || mutation.status === 'rejected';
  const editable = pending && !busy && !removed;
  const dirty = !sameEdits(current, gmailEmailEditsFor(variant)) || selectedId !== review.selectedVariantId || deliveryMode !== review.deliveryMode;
  const setField = (key: keyof GmailEmailEdits) => (value: string) => {
    setSaved(false);
    setEdits((all) => ({ ...all, [variant.id]: { ...(all[variant.id] ?? gmailEmailEditsFor(variant)), [key]: value } }));
  };

  const save = async () => {
    setError('');
    try {
      await onSave(gmailEmailUpdateInput(variant, current, deliveryMode));
      setSaved(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <View style={[styles.card, { backgroundColor: theme.backgroundElement }, removed && styles.cardRemoved]}>
      <View style={styles.cardHeader}>
        <View style={styles.cardHeaderCopy}>
          <ThemedText type="smallBold">{deliveryMode === 'draft' ? 'Save as Gmail draft' : 'Send email'}</ThemedText>
          <ThemedText type="small" themeColor="textSecondary">{mutation.account}</ThemedText>
        </View>
        <StatusPill status={mutation.status} />
      </View>
      {requestReason ? <ThemedText type="small" themeColor="textSecondary">{requestReason}</ThemedText> : null}

      {pending && !removed ? (
        <View style={styles.modeRow}>
          {(['send', 'draft'] as DeliveryMode[]).map((mode) => (
            <Pressable
              key={mode}
              accessibilityRole="button"
              accessibilityState={{ selected: deliveryMode === mode }}
              disabled={!editable}
              onPress={() => { setSaved(false); setDeliveryMode(mode); }}
              style={[styles.modeChip, { backgroundColor: theme.background }, deliveryMode === mode && styles.modeChipActive]}>
              <ThemedText type="small" style={deliveryMode === mode ? styles.modeChipActiveText : undefined}>{mode === 'send' ? 'Send on approval' : 'Save as draft'}</ThemedText>
            </Pressable>
          ))}
        </View>
      ) : null}

      {review.hasVariants ? (
        <View style={styles.variantBlock}>
          <ThemedText type="small" themeColor="textSecondary">{review.variants.length} versions proposed — pick one</ThemedText>
          <View style={styles.modeRow}>
            {review.variants.map((item) => (
              <Pressable
                key={item.id}
                accessibilityRole="button"
                accessibilityState={{ selected: item.id === variant.id }}
                disabled={!editable}
                onPress={() => { setSaved(false); setSelectedId(item.id); }}
                style={[styles.modeChip, { backgroundColor: theme.background }, item.id === variant.id && styles.modeChipActive]}>
                <ThemedText type="small" style={item.id === variant.id ? styles.modeChipActiveText : undefined}>{item.title}</ThemedText>
              </Pressable>
            ))}
          </View>
        </View>
      ) : null}

      <View style={[styles.fields, { backgroundColor: theme.background }]}>
        <Field label="To" value={current.to} onChange={setField('to')} editable={editable} keyboardType="email-address" />
        <Field label="Cc" value={current.cc} onChange={setField('cc')} editable={editable} keyboardType="email-address" />
        <Field label="Bcc" value={current.bcc} onChange={setField('bcc')} editable={editable} keyboardType="email-address" />
        <Field label="Subject" value={current.subject} onChange={setField('subject')} editable={editable} autoCapitalize="sentences" />
        <TextInput
          accessibilityLabel="Email body"
          value={current.editorText}
          onChangeText={setField('editorText')}
          editable={editable}
          multiline
          scrollEnabled={false}
          textAlignVertical="top"
          placeholder="Write the email"
          placeholderTextColor={theme.textSecondary}
          style={[styles.body, { color: theme.text }]}
        />
        {variant.signatureText ? (
          <View style={[styles.signature, { borderTopColor: theme.backgroundSelected }]}>
            <ThemedText type="small" themeColor="textSecondary" selectable>{variant.signatureText}</ThemedText>
          </View>
        ) : null}
        {variant.quotedText ? (
          <View style={[styles.signature, { borderTopColor: theme.backgroundSelected }]}>
            <Pressable accessibilityRole="button" accessibilityState={{ expanded: quotedOpen }} onPress={() => setQuotedOpen((value) => !value)} style={styles.quoteToggle}>
              <ThemedText type="small" style={styles.link}>{quotedOpen ? 'Hide quoted thread' : 'Show quoted thread'}</ThemedText>
            </Pressable>
            {quotedOpen ? <ThemedText type="small" themeColor="textSecondary" selectable>{variant.quotedText}</ThemedText> : null}
          </View>
        ) : null}
      </View>

      {review.replyThreads.map((thread) => <ReplyThread key={thread.threadId} thread={thread} account={mutation.account} />)}

      {mutation.error ? <ThemedText style={styles.error}>{mutation.error}</ThemedText> : null}
      {error ? <ThemedText style={styles.error}>{error}</ThemedText> : null}
      {saved && !dirty ? <ThemedText type="small" style={styles.saved}>Saved. Approval {deliveryMode === 'draft' ? 'saves this draft' : 'sends this version'}.</ThemedText> : null}

      {pending && !removed ? (
        <View style={styles.actions}>
          <Pressable
            accessibilityRole="button"
            onPress={save}
            disabled={!editable || !dirty}
            style={[styles.saveButton, (!editable || !dirty) && styles.disabled]}>
            <ThemedText style={styles.saveText}>{review.hasVariants ? 'Use this version' : 'Save changes'}</ThemedText>
          </Pressable>
          <Pressable accessibilityRole="button" onPress={onRemove} disabled={!editable} style={styles.linkButton}>
            <ThemedText type="smallBold" style={styles.danger}>Don’t send this one</ThemedText>
          </Pressable>
        </View>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  card: { borderRadius: 12, padding: Spacing.three, gap: Spacing.two },
  cardRemoved: { opacity: 0.5 },
  cardHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', gap: Spacing.two },
  cardHeaderCopy: { flex: 1, minWidth: 0, gap: 2 },
  modeRow: { flexDirection: 'row', flexWrap: 'wrap', gap: Spacing.two },
  modeChip: { minHeight: 34, justifyContent: 'center', paddingHorizontal: 12, borderRadius: 17 },
  modeChipActive: { backgroundColor: ACCENT },
  modeChipActiveText: { color: '#FFFFFF', fontWeight: '600' },
  variantBlock: { gap: Spacing.one },
  fields: { borderRadius: 10, paddingHorizontal: Spacing.three },
  fieldRow: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two, minHeight: 44, borderBottomWidth: StyleSheet.hairlineWidth },
  fieldLabel: { width: 52 },
  fieldInput: { flex: 1, minHeight: 44, fontSize: 15 },
  body: { minHeight: 140, paddingVertical: 12, fontSize: 16, lineHeight: 24 },
  signature: { borderTopWidth: StyleSheet.hairlineWidth, paddingVertical: 10, gap: 6 },
  quoteToggle: { minHeight: 32, justifyContent: 'center', alignSelf: 'flex-start' },
  thread: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 10, paddingHorizontal: 12 },
  threadHead: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two, minHeight: 56, paddingVertical: 8 },
  threadCopy: { flex: 1, minWidth: 0, gap: 1 },
  threadMessage: { borderTopWidth: StyleSheet.hairlineWidth, paddingVertical: 10, gap: 6 },
  threadMessageHead: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two },
  threadBody: { fontSize: 15, lineHeight: 22 },
  chevron: { fontSize: 20, lineHeight: 22 },
  chevronOpen: { transform: [{ rotate: '90deg' }], color: ACCENT },
  actions: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', gap: Spacing.three, paddingTop: Spacing.one },
  saveButton: { minHeight: 44, paddingHorizontal: 18, borderRadius: 12, justifyContent: 'center', alignItems: 'center', backgroundColor: ACCENT },
  saveText: { color: '#fff', fontWeight: '600', fontSize: 15 },
  disabled: { opacity: 0.5 },
  linkButton: { minHeight: 44, justifyContent: 'center' },
  link: { color: '#3c87f7' },
  danger: { color: DANGER },
  saved: { color: '#16A34A' },
  error: { color: '#D0342C' },
});
