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
  splitIncomingEmailText,
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
// what a reviewer checks the draft against, so its first lines show by
// default and the rest of the thread is a tap away. Each message's quoted
// history ("On … wrote:" and the "> " lines under it) folds behind its own
// link — on a forwarded thread it was most of the screen.
function ReplyThread({ thread, account }: { thread: GmailEmailReplyThread; account: string }) {
  const theme = useTheme();
  const [open, setOpen] = useState(false);
  const [quotesOpen, setQuotesOpen] = useState<Record<string, boolean>>({});
  const latest = thread.messages[thread.messages.length - 1];
  const earlier = thread.messages.length - 1;
  const shown = open ? thread.messages : latest ? [latest] : [];
  return (
    <View style={[styles.thread, { borderColor: theme.backgroundSelected }]}>
      <Pressable accessibilityRole="button" accessibilityState={{ expanded: open }} accessibilityHint={open ? 'Shows only the latest message' : 'Shows the whole thread'} onPress={() => setOpen((value) => !value)} style={styles.threadHead}>
        <View style={styles.threadCopy}>
          <ThemedText type="small" themeColor="textSecondary">REPLYING TO</ThemedText>
          <ThemedText type="smallBold" numberOfLines={open ? undefined : 1}>{thread.subject || '(no subject)'}</ThemedText>
        </View>
        <ThemedText type="small" style={styles.link}>{open ? 'Less' : earlier > 0 ? `+${earlier} earlier` : 'More'}</ThemedText>
      </Pressable>
      {shown.map((message, index) => {
        const key = message.messageId || String(index);
        const raw = message.hasFullBody ? message.text : cleanSnippet(message.text ?? '').trim();
        const { body, quoted } = splitIncomingEmailText(raw ?? '');
        const quoteOpen = Boolean(quotesOpen[key]);
        return (
          <Pressable key={key} accessibilityRole="button" onPress={() => setOpen((value) => !value)} style={[styles.threadMessage, { borderTopColor: theme.backgroundSelected }]}>
            <View style={styles.threadMessageHead}>
              <Avatar name={message.senderName} size={28} />
              <View style={styles.threadCopy}>
                <ThemedText type="smallBold">{message.senderName}</ThemedText>
                <ThemedText type="small" themeColor="textSecondary" numberOfLines={1}>to {message.to.join(', ') || account}</ThemedText>
              </View>
              <ThemedText type="small" themeColor="textSecondary">{formatWhen(message.sentAt)}</ThemedText>
            </View>
            {body ? <ThemedText selectable={open} numberOfLines={open ? undefined : 3} style={styles.threadBody}>{body}</ThemedText> : null}
            {open && quoted ? (
              <Pressable accessibilityRole="button" accessibilityState={{ expanded: quoteOpen }} hitSlop={6} onPress={() => setQuotesOpen((all) => ({ ...all, [key]: !quoteOpen }))} style={styles.quoteToggle}>
                <ThemedText type="small" style={styles.link}>{quoteOpen ? 'Hide quoted text' : 'Show quoted text'}</ThemedText>
              </Pressable>
            ) : null}
            {open && quoted && quoteOpen ? <ThemedText type="small" themeColor="textSecondary" selectable>{quoted}</ThemedText> : null}
            {open && !message.hasFullBody ? <ThemedText type="small" themeColor="textSecondary">Only a preview is available.</ThemedText> : null}
          </Pressable>
        );
      })}
    </View>
  );
}

type InputFocus = { onFocus: () => void; onBlur: () => void };

function Field({ label, value, onChange, editable, focus, autoCapitalize, keyboardType, accessory }: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  editable: boolean;
  focus: InputFocus;
  autoCapitalize?: 'none' | 'sentences';
  keyboardType?: 'default' | 'email-address';
  accessory?: ReactNode;
}) {
  const theme = useTheme();
  // Every field wraps rather than truncating: a long subject was cut at
  // "submis…" and a long address at "haven.hack…". submitBehavior keeps
  // Return from typing a newline into a one-line value.
  return (
    <View style={[styles.fieldRow, { borderBottomColor: theme.backgroundSelected }]}>
      <ThemedText type="small" themeColor="textSecondary" style={styles.fieldLabel}>{label}</ThemedText>
      <TextInput
        accessibilityLabel={label}
        value={value}
        onChangeText={onChange}
        editable={editable}
        onFocus={focus.onFocus}
        onBlur={focus.onBlur}
        autoCapitalize={autoCapitalize ?? 'none'}
        autoCorrect={autoCapitalize === 'sentences'}
        keyboardType={keyboardType ?? 'default'}
        multiline
        scrollEnabled={false}
        submitBehavior="blurAndSubmit"
        style={[styles.fieldInput, { color: theme.text }]}
      />
      {accessory}
    </View>
  );
}

// One gmail.send_email mutation, rendered as the composer it is: what it
// replies to first, then recipients, subject and the body, all editable in
// place; the signature and quoted thread sit below read-only exactly as they
// will be sent, and a proposal with several variants is chosen from here.
//
// Approval runs the stored version, never the screen's, so the card reports
// the update its edits amount to (onPendingChange) and the screen saves it on
// the way to sending — there is no separate Save step to find.
//
// `locked` is the screen's scroll lock: while the page moves the inputs are
// not focusable, so a touch that stops a scroll cannot raise the keyboard. A
// focused input stays editable through a scroll.
//
// `alone` is the common case — a request that is exactly this email — where
// the screen's header already names the request and its status, so the card
// drops its own header and the agent's note rather than repeating them.
export function GmailEmailComposeCard({
  mutation,
  pending,
  busy,
  onRemove,
  onPendingChange,
  requestReason,
  alone,
  locked,
}: {
  mutation: Mutation;
  pending: boolean;
  busy: boolean;
  onRemove: () => void;
  onPendingChange?: (input: UpdateEmailMutationInput | null) => void;
  requestReason?: string;
  alone?: boolean;
  locked?: boolean;
}) {
  const theme = useTheme();
  const review = useMemo(() => gmailEmailReview(mutation), [mutation]);
  const [selectedId, setSelectedId] = useState(review.selectedVariantId);
  const [deliveryMode, setDeliveryMode] = useState<DeliveryMode>(review.deliveryMode);
  const [edits, setEdits] = useState<Record<string, GmailEmailEdits>>(() => Object.fromEntries(review.variants.map((variant) => [variant.id, gmailEmailEditsFor(variant)])));
  const [quotedOpen, setQuotedOpen] = useState(false);
  const [signatureOpen, setSignatureOpen] = useState(false);
  const [copiesOpen, setCopiesOpen] = useState(false);
  const [focused, setFocused] = useState(false);
  const focus = useMemo<InputFocus>(() => ({ onFocus: () => setFocused(true), onBlur: () => setFocused(false) }), []);
  // A new server baseline (the save on the way to sending, or any reload)
  // replaces the local edits, so the card never reports a difference the
  // server has already absorbed — its normalisation of the saved text would
  // otherwise leave "Edited" up and save a second time. Reset during render,
  // not in an effect, as the Slack card does.
  const baselineSignature = `${review.deliveryMode}|${review.selectedVariantId}|${JSON.stringify(review.variants.map(gmailEmailEditsFor))}`;
  const [baseline, setBaseline] = useState(baselineSignature);
  if (baseline !== baselineSignature) {
    setBaseline(baselineSignature);
    setSelectedId(review.selectedVariantId);
    setDeliveryMode(review.deliveryMode);
    setEdits(Object.fromEntries(review.variants.map((variant) => [variant.id, gmailEmailEditsFor(variant)])));
  }

  const variant: GmailEmailVariant = review.variants.find((item) => item.id === selectedId) ?? review.variants[0];
  const current = edits[variant.id] ?? gmailEmailEditsFor(variant);
  const removed = mutation.status === 'removed' || mutation.status === 'skipped' || mutation.status === 'rejected';
  const editable = pending && !busy && !removed;
  const inputsEditable = editable && (focused || !locked);
  const dirty = !sameEdits(current, gmailEmailEditsFor(variant)) || selectedId !== review.selectedVariantId || deliveryMode !== review.deliveryMode;
  // Empty Cc and Bcc rows were blank lines on every email; each appears when
  // it holds something or when asked for.
  const showCc = copiesOpen || Boolean(current.cc.trim());
  const showBcc = copiesOpen || Boolean(current.bcc.trim());
  const signatureSummary = emailSignatureSummary(variant.signatureText);

  const pendingInput = useMemo(
    () => (editable && dirty ? gmailEmailUpdateInput(variant, current, deliveryMode) : null),
    [editable, dirty, variant, current, deliveryMode],
  );
  // The callback is held in a ref: the parent passes a fresh closure every
  // render, and keying the effects on it made each report re-render the
  // parent, which re-reported — an update loop on the first keystroke.
  const report = useRef(onPendingChange);
  useEffect(() => {
    report.current = onPendingChange;
  }, [onPendingChange]);
  useEffect(() => {
    report.current?.(pendingInput);
  }, [pendingInput]);
  useEffect(() => {
    const reporter = report;
    return () => reporter.current?.(null);
  }, []);

  const setField = (key: keyof GmailEmailEdits) => (value: string) => {
    setEdits((all) => ({ ...all, [variant.id]: { ...(all[variant.id] ?? gmailEmailEditsFor(variant)), [key]: value } }));
  };
  const revert = () => {
    setSelectedId(review.selectedVariantId);
    setDeliveryMode(review.deliveryMode);
    setEdits(Object.fromEntries(review.variants.map((item) => [item.id, gmailEmailEditsFor(item)])));
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
      {review.forward ? (
        <View style={styles.variantBlock}>
          <ThemedText type="smallBold">{review.forward.heading}</ThemedText>
          <ThemedText type="small" themeColor="textSecondary">{review.forward.filesText}</ThemedText>
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
                onPress={() => setSelectedId(item.id)}
                style={[styles.modeChip, { backgroundColor: theme.backgroundElement }, item.id === variant.id && styles.modeChipActive]}>
                <ThemedText type="small" style={item.id === variant.id ? styles.modeChipActiveText : undefined}>{item.title}</ThemedText>
              </Pressable>
            ))}
          </View>
        </View>
      ) : null}

      {alone ? <ThemedText type="small" themeColor="textSecondary" numberOfLines={1}>From {mutation.account}</ThemedText> : null}
      <View style={[styles.fields, { backgroundColor: alone ? theme.backgroundElement : theme.background }]}>
        <Field
          label="To"
          value={current.to}
          onChange={setField('to')}
          editable={inputsEditable}
          focus={focus}
          keyboardType="email-address"
          accessory={!(showCc && showBcc) && editable ? (
            <Pressable accessibilityRole="button" accessibilityLabel="Add Cc or Bcc" onPress={() => setCopiesOpen(true)} hitSlop={8}>
              <ThemedText type="small" style={styles.link}>Cc/Bcc</ThemedText>
            </Pressable>
          ) : null}
        />
        {showCc ? <Field label="Cc" value={current.cc} onChange={setField('cc')} editable={inputsEditable} focus={focus} keyboardType="email-address" /> : null}
        {showBcc ? <Field label="Bcc" value={current.bcc} onChange={setField('bcc')} editable={inputsEditable} focus={focus} keyboardType="email-address" /> : null}
        <Field label="Subject" value={current.subject} onChange={setField('subject')} editable={inputsEditable} focus={focus} autoCapitalize="sentences" />
        <TextInput
          accessibilityLabel="Email body"
          value={current.editorText}
          onChangeText={setField('editorText')}
          editable={inputsEditable}
          onFocus={focus.onFocus}
          onBlur={focus.onBlur}
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
              <ThemedText type="small" style={styles.link}>{`${quotedOpen ? 'Hide' : 'Show'} ${review.forward?.quoteLabel ?? 'quoted thread'}`}</ThemedText>
            </Pressable>
            {quotedOpen ? <ThemedText type="small" themeColor="textSecondary" selectable>{variant.quotedText}</ThemedText> : null}
          </View>
        ) : null}
      </View>

      {editable && dirty ? (
        <View style={styles.editedRow}>
          <ThemedText type="small" themeColor="textSecondary">Edited · saved when you {deliveryMode === 'draft' ? 'save the draft' : 'send'}</ThemedText>
          <Pressable accessibilityRole="button" onPress={revert} hitSlop={8}>
            <ThemedText type="smallBold" style={styles.link}>Revert</ThemedText>
          </Pressable>
        </View>
      ) : null}

      {pending && !removed ? (
        <View style={styles.modeRow}>
          {(['send', 'draft'] as DeliveryMode[]).map((mode) => (
            <Pressable
              key={mode}
              accessibilityRole="button"
              accessibilityState={{ selected: deliveryMode === mode }}
              disabled={!editable}
              onPress={() => setDeliveryMode(mode)}
              style={[styles.modeChip, { backgroundColor: alone ? theme.backgroundElement : theme.background }, deliveryMode === mode && styles.modeChipActive]}>
              <ThemedText type="small" style={deliveryMode === mode ? styles.modeChipActiveText : undefined}>{mode === 'send' ? 'Send on approval' : 'Save as draft'}</ThemedText>
            </Pressable>
          ))}
        </View>
      ) : null}

      {mutation.error ? <ThemedText style={styles.error}>{mutation.error}</ThemedText> : null}

      {/* Only a request of several emails can drop one; for one email that is Don't send. */}
      {editable && !alone ? (
        <Pressable accessibilityRole="button" onPress={onRemove} style={styles.linkButton}>
          <ThemedText type="smallBold" style={styles.danger}>Don’t send this one</ThemedText>
        </Pressable>
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
  fieldInput: { flex: 1, minHeight: 44, fontSize: 15, paddingVertical: 12 },
  body: { minHeight: 140, paddingVertical: 12, fontSize: 16, lineHeight: 24 },
  signature: { borderTopWidth: StyleSheet.hairlineWidth, paddingVertical: 10, gap: 6 },
  quoteToggle: { minHeight: 32, justifyContent: 'center', alignSelf: 'flex-start' },
  thread: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 10, paddingHorizontal: 12 },
  threadHead: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two, minHeight: 56, paddingVertical: 8 },
  threadCopy: { flex: 1, minWidth: 0, gap: 1 },
  threadMessage: { borderTopWidth: StyleSheet.hairlineWidth, paddingVertical: 10, gap: 6 },
  threadMessageHead: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two },
  threadBody: { fontSize: 15, lineHeight: 22 },
  editedRow: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', gap: Spacing.three },
  linkButton: { minHeight: 44, justifyContent: 'center' },
  link: { color: '#3c87f7' },
  danger: { color: DANGER },
  error: { color: '#D0342C' },
});
