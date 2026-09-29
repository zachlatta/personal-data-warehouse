import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { Pressable, StyleSheet, TextInput, View } from 'react-native';

import { EmailAttachments } from '@/components/email-attachment-preview';
import { Avatar } from '@/components/avatar';
import { StatusPill } from '@/components/status-pill';
import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import type { Mutation, UpdateEmailMutationInput } from '@/lib/api';
import { cleanSnippet, formatWhen } from '@/lib/format';
import {
  emailSignatureSummary,
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

// What is being replied to, above the reply: the message just answered is
// what a reviewer checks the draft against, so it is open by default (a few
// lines of it), and the rest of the thread is a tap away. It used to sit
// below the signature, closed, as "REPLYING IN THREAD · 1 message".
function ReplyThread({ thread, account }: { thread: GmailEmailReplyThread; account: string }) {
  const theme = useTheme();
  const [open, setOpen] = useState(false);
  const latest = thread.messages[thread.messages.length - 1];
  const earlier = thread.messages.length - 1;
  const shown = open ? thread.messages : latest ? [latest] : [];
  return (
    <View style={[styles.thread, { borderColor: theme.backgroundSelected }]}>
      <Pressable accessibilityRole="button" accessibilityState={{ expanded: open }} accessibilityHint={open ? 'Shows only the latest message' : 'Shows the whole thread'} onPress={() => setOpen((value) => !value)} style={styles.threadHead}>
        <View style={styles.threadCopy}>
          <ThemedText type="small" themeColor="textSecondary">REPLYING TO</ThemedText>
          <ThemedText type="smallBold" numberOfLines={open ? undefined : 2}>{thread.subject || '(no subject)'}</ThemedText>
        </View>
        <ThemedText type="small" style={styles.link}>{open ? 'Less' : earlier > 0 ? `+${earlier} earlier` : 'More'}</ThemedText>
      </Pressable>
      {shown.map((message, index) => {
        const body = message.hasFullBody ? message.text : cleanSnippet(message.text ?? '').trim();
        return (
          <Pressable key={message.messageId || index} accessibilityRole="button" onPress={() => setOpen((value) => !value)} style={[styles.threadMessage, { borderTopColor: theme.backgroundSelected }]}>
            <View style={styles.threadMessageHead}>
              <Avatar name={message.senderName} size={28} />
              <View style={styles.threadCopy}>
                <ThemedText type="smallBold">{message.senderName}</ThemedText>
                <ThemedText type="small" themeColor="textSecondary" numberOfLines={1}>to {message.to.join(', ') || account}</ThemedText>
              </View>
              <ThemedText type="small" themeColor="textSecondary">{formatWhen(message.sentAt)}</ThemedText>
            </View>
            {body ? <ThemedText selectable={open} numberOfLines={open ? undefined : 5} style={styles.threadBody}>{body}</ThemedText> : null}
            {open && !message.hasFullBody ? <ThemedText type="small" themeColor="textSecondary">Only a preview is available.</ThemedText> : null}
          </Pressable>
        );
      })}
    </View>
  );
}

function Field({ label, value, onChange, editable, autoCapitalize, keyboardType, wrap, accessory }: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  editable: boolean;
  autoCapitalize?: 'none' | 'sentences';
  keyboardType?: 'default' | 'email-address';
  // Wrap instead of truncating: a long subject was cut at "submis…".
  wrap?: boolean;
  accessory?: ReactNode;
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
        multiline={wrap}
        scrollEnabled={wrap ? false : undefined}
        submitBehavior={wrap ? 'blurAndSubmit' : undefined}
        style={[styles.fieldInput, wrap && styles.fieldInputWrap, { color: theme.text }]}
      />
      {accessory}
    </View>
  );
}

// One gmail.send_email mutation, rendered as the composer it is: what it
// replies to first, then recipients, subject and the body, editable; the
// signature and quoted thread sit below read-only exactly as they will be
// sent, and a proposal with several variants is chosen from here. Saving
// posts to update-email, so the edit is on the server before the request is
// approved — approval sends what is stored, never what is on screen, which is
// why the card reports unsaved edits up to the screen that owns Approve.
//
// `alone` is the common case — a request that is exactly this email — where
// the screen's header already names the request and its status, so the card
// drops its own header and the agent's note rather than repeating them.
export function GmailEmailComposeCard({
  mutation,
  pending,
  busy,
  onSave,
  onRemove,
  onDirtyChange,
  requestReason,
  alone,
}: {
  mutation: Mutation;
  pending: boolean;
  busy: boolean;
  onSave: (input: UpdateEmailMutationInput) => Promise<void>;
  onRemove: () => void;
  onDirtyChange?: (dirty: boolean) => void;
  requestReason?: string;
  alone?: boolean;
}) {
  const theme = useTheme();
  const review = useMemo(() => gmailEmailReview(mutation), [mutation]);
  const [selectedId, setSelectedId] = useState(review.selectedVariantId);
  const [deliveryMode, setDeliveryMode] = useState<DeliveryMode>(review.deliveryMode);
  const [edits, setEdits] = useState<Record<string, GmailEmailEdits>>(() => Object.fromEntries(review.variants.map((variant) => [variant.id, gmailEmailEditsFor(variant)])));
  const [quotedOpen, setQuotedOpen] = useState(false);
  const [signatureOpen, setSignatureOpen] = useState(false);
  const [copiesOpen, setCopiesOpen] = useState(false);
  const [error, setError] = useState('');
  const [saved, setSaved] = useState(false);

  const variant: GmailEmailVariant = review.variants.find((item) => item.id === selectedId) ?? review.variants[0];
  const current = edits[variant.id] ?? gmailEmailEditsFor(variant);
  const removed = mutation.status === 'removed' || mutation.status === 'skipped' || mutation.status === 'rejected';
  const editable = pending && !busy && !removed;
  const dirty = !sameEdits(current, gmailEmailEditsFor(variant)) || selectedId !== review.selectedVariantId || deliveryMode !== review.deliveryMode;
  // Empty Cc and Bcc rows were two blank lines on every email; they appear
  // when they hold something or when asked for.
  const showCopies = copiesOpen || Boolean(current.cc.trim() || current.bcc.trim());
  const signatureSummary = emailSignatureSummary(variant.signatureText);

  // The callback is held in a ref: the parent passes a fresh closure every
  // render, and keying the effects on it made each report re-render the
  // parent, which re-reported — an update loop on the first keystroke.
  const reportDirty = useRef(onDirtyChange);
  useEffect(() => {
    reportDirty.current = onDirtyChange;
  }, [onDirtyChange]);
  const unsaved = editable && dirty;
  useEffect(() => {
    reportDirty.current?.(unsaved);
  }, [unsaved]);
  useEffect(() => {
    const report = reportDirty;
    return () => report.current?.(false);
  }, []);

  const setField = (key: keyof GmailEmailEdits) => (value: string) => {
    setSaved(false);
    setEdits((all) => ({ ...all, [variant.id]: { ...(all[variant.id] ?? gmailEmailEditsFor(variant)), [key]: value } }));
  };
  const revert = () => {
    setSaved(false);
    setError('');
    setSelectedId(review.selectedVariantId);
    setDeliveryMode(review.deliveryMode);
    setEdits(Object.fromEntries(review.variants.map((item) => [item.id, gmailEmailEditsFor(item)])));
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
    <View style={[styles.card, alone && styles.cardAlone, { backgroundColor: alone ? undefined : theme.backgroundElement }, removed && styles.cardRemoved]}>
      {alone ? null : (
        <View style={styles.cardHeader}>
          <View style={styles.cardHeaderCopy}>
            <ThemedText type="smallBold">{deliveryMode === 'draft' ? 'Save as Gmail draft' : 'Send email'}</ThemedText>
            <ThemedText type="small" themeColor="textSecondary">{mutation.account}</ThemedText>
          </View>
          <StatusPill status={mutation.status} />
        </View>
      )}
      {!alone && requestReason ? <ThemedText type="small" themeColor="textSecondary">{requestReason}</ThemedText> : null}

      {review.replyThreads.map((thread) => <ReplyThread key={thread.threadId} thread={thread} account={mutation.account} />)}

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
                style={[styles.modeChip, { backgroundColor: theme.backgroundElement }, item.id === variant.id && styles.modeChipActive]}>
                <ThemedText type="small" style={item.id === variant.id ? styles.modeChipActiveText : undefined}>{item.title}</ThemedText>
              </Pressable>
            ))}
          </View>
        </View>
      ) : null}

      <View style={[styles.fields, { backgroundColor: alone ? theme.backgroundElement : theme.background }]}>
        {alone ? (
          <View style={[styles.fieldRow, { borderBottomColor: theme.backgroundSelected }]}>
            <ThemedText type="small" themeColor="textSecondary" style={styles.fieldLabel}>From</ThemedText>
            <ThemedText type="small" themeColor="textSecondary" style={styles.fieldStatic} numberOfLines={1}>{mutation.account}</ThemedText>
          </View>
        ) : null}
        <Field
          label="To"
          value={current.to}
          onChange={setField('to')}
          editable={editable}
          keyboardType="email-address"
          accessory={!showCopies && editable ? (
            <Pressable accessibilityRole="button" accessibilityLabel="Add Cc or Bcc" onPress={() => setCopiesOpen(true)} hitSlop={8}>
              <ThemedText type="small" style={styles.link}>Cc/Bcc</ThemedText>
            </Pressable>
          ) : null}
        />
        {showCopies ? <Field label="Cc" value={current.cc} onChange={setField('cc')} editable={editable} keyboardType="email-address" /> : null}
        {showCopies ? <Field label="Bcc" value={current.bcc} onChange={setField('bcc')} editable={editable} keyboardType="email-address" /> : null}
        <Field label="Subject" value={current.subject} onChange={setField('subject')} editable={editable} autoCapitalize="sentences" wrap />
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
        {variant.attachments.length > 0 ? (
          <View style={[styles.signature, { borderTopColor: theme.backgroundSelected }]}>
            <EmailAttachments key={variant.id} attachments={variant.attachments} />
          </View>
        ) : null}
        {variant.signatureText ? (
          <View style={[styles.signature, { borderTopColor: theme.backgroundSelected }]}>
            <Pressable accessibilityRole="button" accessibilityState={{ expanded: signatureOpen }} accessibilityLabel={signatureOpen ? 'Hide signature' : 'Show signature'} onPress={() => setSignatureOpen((value) => !value)} style={styles.quoteToggle}>
              {signatureOpen
                ? <ThemedText type="small" themeColor="textSecondary" selectable>{variant.signatureText}</ThemedText>
                : <ThemedText type="small" themeColor="textSecondary" numberOfLines={1}>Signature · {signatureSummary || 'show'}</ThemedText>}
            </Pressable>
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

      {pending && !removed ? (
        <View style={styles.modeRow}>
          {(['send', 'draft'] as DeliveryMode[]).map((mode) => (
            <Pressable
              key={mode}
              accessibilityRole="button"
              accessibilityState={{ selected: deliveryMode === mode }}
              disabled={!editable}
              onPress={() => { setSaved(false); setDeliveryMode(mode); }}
              style={[styles.modeChip, { backgroundColor: alone ? theme.backgroundElement : theme.background }, deliveryMode === mode && styles.modeChipActive]}>
              <ThemedText type="small" style={deliveryMode === mode ? styles.modeChipActiveText : undefined}>{mode === 'send' ? 'Send on approval' : 'Save as draft'}</ThemedText>
            </Pressable>
          ))}
        </View>
      ) : null}

      {mutation.error ? <ThemedText style={styles.error}>{mutation.error}</ThemedText> : null}
      {error ? <ThemedText style={styles.error}>{error}</ThemedText> : null}
      {saved && !dirty ? <ThemedText type="small" style={styles.saved}>Saved. Approval {deliveryMode === 'draft' ? 'saves this draft' : 'sends this version'}.</ThemedText> : null}

      {/* Save appears once there is something to save; the per-email drop is
          only for a request of several emails, where Deny would drop them all. */}
      {editable && (dirty || !alone) ? (
        <View style={styles.actions}>
          {dirty ? (
            <View style={styles.saveGroup}>
              <Pressable accessibilityRole="button" onPress={save} style={styles.saveButton}>
                <ThemedText style={styles.saveText}>{review.hasVariants ? 'Use this version' : 'Save changes'}</ThemedText>
              </Pressable>
              <Pressable accessibilityRole="button" onPress={revert} style={styles.linkButton} hitSlop={6}>
                <ThemedText type="small" themeColor="textSecondary">Revert</ThemedText>
              </Pressable>
            </View>
          ) : <View />}
          {!alone ? (
            <Pressable accessibilityRole="button" onPress={onRemove} style={styles.linkButton}>
              <ThemedText type="smallBold" style={styles.danger}>Don’t send this one</ThemedText>
            </Pressable>
          ) : null}
        </View>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  card: { borderRadius: 12, padding: Spacing.three, gap: Spacing.two },
  cardAlone: { padding: 0, gap: Spacing.three },
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
  fieldInputWrap: { paddingVertical: 12 },
  fieldStatic: { flex: 1 },
  body: { minHeight: 140, paddingVertical: 12, fontSize: 16, lineHeight: 24 },
  signature: { borderTopWidth: StyleSheet.hairlineWidth, paddingVertical: 10, gap: 6 },
  quoteToggle: { minHeight: 32, justifyContent: 'center', alignSelf: 'flex-start' },
  thread: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 10, paddingHorizontal: 12 },
  threadHead: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two, minHeight: 56, paddingVertical: 8 },
  threadCopy: { flex: 1, minWidth: 0, gap: 1 },
  threadMessage: { borderTopWidth: StyleSheet.hairlineWidth, paddingVertical: 10, gap: 6 },
  threadMessageHead: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two },
  threadBody: { fontSize: 15, lineHeight: 22 },
  actions: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', gap: Spacing.three, paddingTop: Spacing.one },
  saveButton: { minHeight: 44, paddingHorizontal: 18, borderRadius: 12, justifyContent: 'center', alignItems: 'center', backgroundColor: ACCENT },
  saveText: { color: '#fff', fontWeight: '600', fontSize: 15 },
  saveGroup: { flexDirection: 'row', alignItems: 'center', gap: Spacing.three },
  linkButton: { minHeight: 44, justifyContent: 'center' },
  link: { color: '#3c87f7' },
  danger: { color: DANGER },
  saved: { color: '#16A34A' },
  error: { color: '#D0342C' },
});
