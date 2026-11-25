import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import test from 'node:test';
import vm from 'node:vm';
import ts from 'typescript';

import {
  EMAIL_EDITOR_SCRIPT,
  emailEditorDocument,
  emailEditorHtmlFromMessage,
  emailEditorResetKey,
  stripTrailingEmptyBlocksHtml,
} from './email-editor.ts';

const require = createRequire(import.meta.url);

// Runs the real EMAIL_EDITOR_SCRIPT in a minimal sandboxed DOM so tests exercise
// actual script runtime behavior (focus/blur, input, revert, snapshot) rather than
// regex checks on the script source.
function runEditorScript(initialInnerHTML) {
  const listeners = {};
  const messages = [];
  const editor = {
    innerHTML: initialInnerHTML,
    firstChild: null,
    getBoundingClientRect: () => ({ height: 220 }),
    getAttribute: (k) => (k === 'contenteditable' ? 'true' : null),
    setAttribute: () => {},
    addEventListener: (ev, fn) => {
      (listeners[ev] ||= []).push(fn);
    },
  };
  const windowObj = {
    ReactNativeWebView: { postMessage: (raw) => messages.push(JSON.parse(raw)) },
    addEventListener: () => {},
  };
  const sandbox = {
    window: windowObj,
    document: {
      getElementById: (id) => (id === 'editor' ? editor : null),
      execCommand: () => {},
    },
    setTimeout: () => 0,
  };
  vm.runInNewContext(EMAIL_EDITOR_SCRIPT, sandbox);
  const fire = (ev, arg = {}) => {
    for (const fn of listeners[ev] || []) fn(arg);
  };
  return { editor, windowObj, messages, fire };
}

test('emailEditorDocument embeds server HTML without double-escaping attribute entities and enforces script-src none CSP', () => {
  const serverHtml =
    '<p>Hi <strong>Alex</strong>, <a href="https://example.org/x?a=1&amp;b=2">link</a></p>' +
    '<p style="font-family:&#34;Helvetica Neue&#34;,Arial"><img src="http://example.org/img?x=1&amp;w=2" alt="A &amp; B"></p>' +
    '<p>Set once = 5 and only=yes, then onclick="x" in the docs.</p>';
  const doc = emailEditorDocument(serverHtml, true);
  assert.match(doc, /Content-Security-Policy" content="default-src 'none'; script-src 'none'/);
  assert.match(doc, /background:#FFFFFF/);
  assert.match(doc, /color:#111111/);
  assert.match(doc, /id="editor" contenteditable="true"/);
  assert.ok(doc.includes('href="https://example.org/x?a=1&amp;b=2"'));
  assert.ok(!doc.includes('&amp;amp;'));
  assert.ok(doc.includes('src="http://example.org/img?x=1&amp;w=2"'));
  assert.ok(doc.includes('alt="A &amp; B"'));
  assert.ok(doc.includes('font-family:&#34;Helvetica Neue&#34;,Arial'));
  assert.ok(doc.includes('Set once = 5 and only=yes, then onclick="x" in the docs.'));

  const readonlyDoc = emailEditorDocument('<p>Hello</p>', false);
  assert.match(readonlyDoc, /id="editor" contenteditable="false"/);
});

test('R1 real prop flow: type then backspace or undo restores builtHtml (server copy or reopened draft), never stale live draft', () => {
  // Case 1: Built from server copy "<p>could</p>"; type "Q" then backspace.
  const serverCopy = '<p>could</p>';
  const builtHtml1 = serverCopy; // Captured once when resetKey changes.
  let liveDraft1 = serverCopy;
  const { editor: ed1, messages: msgs1, fire: fire1, windowObj: win1 } = runEditorScript(builtHtml1);

  // Focus + blur without typing posts zero change/revert messages.
  fire1('focus');
  fire1('blur');
  assert.equal(msgs1.filter((m) => m.type === 'change' || m.type === 'revert').length, 0);
  assert.equal(liveDraft1, serverCopy);

  // Type "Q" -> liveDraft1 becomes "<p>coQuld</p>".
  ed1.innerHTML = '<p>coQuld</p>';
  fire1('input');
  const changeMsg = msgs1.at(-1);
  assert.equal(changeMsg.type, 'change');
  liveDraft1 = emailEditorHtmlFromMessage(changeMsg, builtHtml1) ?? liveDraft1;
  assert.equal(liveDraft1, '<p>coQuld</p>');

  // Backspace back to "<p>could</p>" -> script posts 'revert'; resolving against
  // builtHtml1 (NOT liveDraft1!) restores serverCopy "<p>could</p>".
  ed1.innerHTML = '<p>could</p>';
  fire1('input');
  const revertMsg = msgs1.at(-1);
  assert.equal(revertMsg.type, 'revert');
  liveDraft1 = emailEditorHtmlFromMessage(revertMsg, builtHtml1) ?? liveDraft1;
  assert.equal(liveDraft1, serverCopy);

  // Snapshot read on approve while keystroke is pending returns live editor HTML.
  ed1.innerHTML = '<p>could!</p>';
  win1.__pdwReadSnapshot(42);
  const snapMsg = msgs1.at(-1);
  assert.deepEqual(snapMsg, { type: 'snapshot', id: 42, html: '<p>could!</p>', isBaseline: false });
  assert.equal(emailEditorHtmlFromMessage(snapMsg, builtHtml1), '<p>could!</p>');

  // Case 2: Built from reopened unsaved draft "<p>coDDuld</p>"; type "E" then backspace.
  const reopenedDraft = '<p>coDDuld</p>';
  const builtHtml2 = reopenedDraft;
  let liveDraft2 = reopenedDraft;
  const { editor: ed2, messages: msgs2, fire: fire2 } = runEditorScript(builtHtml2);
  ed2.innerHTML = '<p>coEDDuld</p>';
  fire2('input');
  liveDraft2 = emailEditorHtmlFromMessage(msgs2.at(-1), builtHtml2) ?? liveDraft2;
  assert.equal(liveDraft2, '<p>coEDDuld</p>');
  ed2.innerHTML = '<p>coDDuld</p>';
  fire2('input');
  assert.equal(msgs2.at(-1).type, 'revert');
  liveDraft2 = emailEditorHtmlFromMessage(msgs2.at(-1), builtHtml2) ?? liveDraft2;
  assert.equal(liveDraft2, reopenedDraft);
});

test('emailEditorResetKey changes when server baseline updates so WebView reloads fresh content', () => {
  const k1 = emailEditorResetKey('req:mut', 'v1', 0, 'baseline-A');
  const k2 = emailEditorResetKey('req:mut', 'v1', 0, 'baseline-B');
  assert.notEqual(k1, k2);
});

test('stripTrailingEmptyBlocksHtml trims cursor-room empty divs without removing content', () => {
  assert.equal(stripTrailingEmptyBlocksHtml('<p>Hello</p><div><br></div><div><br></div>'), '<p>Hello</p>');
  assert.equal(stripTrailingEmptyBlocksHtml('<div><br></div>'), '<div><br></div>');
  assert.equal(stripTrailingEmptyBlocksHtml('<p>One</p><p><br></p><p>Two</p>'), '<p>One</p><p><br></p><p>Two</p>');
});

// Minimal hooks runtime matching mobile/src/features/feed/article-fallback.test.mjs
// so real .tsx components execute directly under node --test with zero new deps.
function makeReactHooks() {
  const slots = [];
  let cursor = 0;
  let pendingEffects = [];
  const depsChanged = (prev, next) =>
    !prev || !next || prev.length !== next.length || next.some((v, i) => !Object.is(v, prev[i]));
  const react = {
    useState(initial) {
      const i = cursor++;
      if (!(i in slots)) slots[i] = typeof initial === 'function' ? initial() : initial;
      return [slots[i], (v) => { slots[i] = typeof v === 'function' ? v(slots[i]) : v; }];
    },
    useRef(initial) {
      const i = cursor++;
      if (!(i in slots)) slots[i] = { current: initial };
      return slots[i];
    },
    useMemo(fn, deps) {
      const i = cursor++;
      if (!(i in slots) || depsChanged(slots[i].deps, deps)) {
        slots[i] = { value: fn(), deps };
      }
      return slots[i].value;
    },
    useCallback(fn, deps) {
      const i = cursor++;
      if (!(i in slots) || depsChanged(slots[i].deps, deps)) {
        slots[i] = { value: fn, deps };
      }
      return slots[i].value;
    },
    useEffect(fn, deps) {
      const i = cursor++;
      if (!(i in slots) || depsChanged(slots[i].deps, deps)) {
        slots[i] = { deps };
        pendingEffects.push(fn);
      }
    },
  };
  return {
    react,
    beginRender() {
      cursor = 0;
      pendingEffects = [];
    },
    flushEffects() {
      const fns = pendingEffects;
      pendingEffects = [];
      for (const fn of fns) fn();
    },
  };
}

function findElements(node, pred, out = []) {
  if (!node || typeof node !== 'object') return out;
  if (Array.isArray(node)) {
    for (const child of node) findElements(child, pred, out);
    return out;
  }
  if (pred(node)) out.push(node);
  if (node.props?.children) findElements(node.props.children, pred, out);
  return out;
}

test('real EmailRichEditor component reverts to builtHtml (not live initialHtml prop) and resolves snapshot reads', async () => {
  const hooks = makeReactHooks();
  const injected = [];
  const webViewInstance = { injectJavaScript: (js) => injected.push(js) };
  const modules = {
    react: hooks.react,
    'react-native': {
      Linking: { openURL: async () => {} },
      Platform: { OS: 'android' },
      Pressable: 'Pressable',
      StyleSheet: { create: (s) => s, hairlineWidth: 1 },
      View: 'View',
    },
    'react-native-webview': { WebView: 'WebView' },
    'expo-web-browser': { openBrowserAsync: async () => {} },
    '@/components/themed-text': { ThemedText: 'ThemedText' },
    '@/hooks/use-theme': { useTheme: () => ({ background: '#fff', backgroundElement: '#eee', backgroundSelected: '#ddd' }) },
    '@/lib/email-editor': {
      EMAIL_EDITOR_SCRIPT,
      emailEditorDocument,
      emailEditorHtmlFromMessage,
    },
    '@/lib/email-html': {
      emailNavigation: () => 'load',
      emailWebViewPlatformProps: () => ({}),
    },
  };
  const source = ts.transpileModule(
    readFileSync(new URL('../components/email-rich-editor.tsx', import.meta.url), 'utf8'),
    { compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX } },
  ).outputText;
  const exports = {};
  vm.runInNewContext(source, { exports, require: (id) => modules[id] ?? require(id), setTimeout, clearTimeout });

  const changes = [];
  let registeredReader = null;
  const render = (initialHtml, resetKey = 'k1') => {
    hooks.beginRender();
    const tree = exports.EmailRichEditor({
      initialHtml,
      resetKey,
      editable: true,
      onChangeHtml: (html) => changes.push(html),
      onFocus: () => {},
      onBlur: () => {},
      onRegisterSnapshotReader: (reader) => { registeredReader = reader; },
    });
    const wv = findElements(tree, (el) => el.type === 'WebView')[0];
    if (wv?.props?.ref) wv.props.ref.current = webViewInstance;
    hooks.flushEffects();
    return wv;
  };

  // Mount with server copy "<p>could</p>".
  let wv = render('<p>could</p>', 'k1');
  // Type "Q" -> change message updates parent draft state to "<p>coQuld</p>".
  wv.props.onMessage({ nativeEvent: { data: JSON.stringify({ type: 'change', html: '<p>coQuld</p>' }) } });
  assert.equal(changes.at(-1), '<p>coQuld</p>');

  // Re-render with live draft prop initialHtml="<p>coQuld</p>" and SAME resetKey="k1".
  wv = render('<p>coQuld</p>', 'k1');
  // Backspace back to baseline -> script sends { type: 'revert' }.
  // Component MUST emit builtHtml ("<p>could</p>"), NEVER the live initialHtml prop ("<p>coQuld</p>").
  wv.props.onMessage({ nativeEvent: { data: JSON.stringify({ type: 'revert' }) } });
  assert.equal(changes.at(-1), '<p>could</p>');

  // Snapshot read resolves from the WebView's live snapshot message.
  assert.equal(typeof registeredReader, 'function');
  const snapPromise = registeredReader();
  const snapCall = injected.find((js) => js.includes('__pdwReadSnapshot('));
  assert.ok(snapCall, 'expected snapshot reader to inject __pdwReadSnapshot');
  const idMatch = snapCall.match(/__pdwReadSnapshot\((\d+)\)/);
  assert.ok(idMatch);
  wv.props.onMessage({
    nativeEvent: { data: JSON.stringify({ type: 'snapshot', id: Number(idMatch[1]), html: '<p>could!</p>', isBaseline: false }) },
  });
  assert.equal(await snapPromise, '<p>could!</p>');
});

test('real MutationReview screen flushes live email editor HTML on Approve before deciding', async () => {
  const mutationReview = require('./mutation-review.ts');
  const makeRequest = () => ({
    id: 'req-1',
    title: 'Send email',
    status: 'pending_review',
    created_at: '2026-11-24T00:00:00Z',
    requested_by: 'agent',
    mutation_count: 1,
    mutations: [
      {
        id: 'mut-1',
        request_id: 'req-1',
        operation: 'gmail.send_email',
        account: 'zach@example.com',
        status: 'pending_review',
        payload: {
          message: {
            to: ['alex@example.com'],
            subject: 'Hello',
            body_html: '<p>could</p>',
            body_text: 'could',
          },
        },
      },
    ],
  });

  function mountScreen() {
    const hooks = makeReactHooks();
    const updatedCalls = [];
    const heldDecisions = [];
    const req = makeRequest();
    const modules = {
      react: hooks.react,
      'react-native': {
        ActivityIndicator: 'ActivityIndicator',
        Alert: { alert: () => {} },
        KeyboardAvoidingView: 'KeyboardAvoidingView',
        Pressable: 'Pressable',
        ScrollView: 'ScrollView',
        SectionList: 'SectionList',
        StyleSheet: { create: (s) => s, hairlineWidth: 1 },
        TextInput: 'TextInput',
        View: 'View',
        useColorScheme: () => 'light',
      },
      'expo-router': {
        Stack: { Screen: 'StackScreen' },
        router: { replace: () => {}, back: () => {}, canGoBack: () => false, push: () => {} },
        useLocalSearchParams: () => ({ id: 'req-1' }),
      },
      'react-native-safe-area-context': { useSafeAreaInsets: () => ({ bottom: 0, top: 0, left: 0, right: 0 }) },
      '@/components/themed-text': { ThemedText: 'ThemedText' },
      '@/components/themed-view': { ThemedView: 'ThemedView' },
      '@/constants/theme': { Palette: {}, Spacing: { one: 4, two: 8, three: 12, four: 16 } },
      '@/hooks/use-scroll-lock': { useScrollLock: () => ({ locked: false, handlers: {} }) },
      '@/hooks/use-theme': { useTheme: () => ({ background: '#fff', backgroundElement: '#eee', backgroundSelected: '#ddd', surface: '#fff', surfaceBorder: '#ccc', text: '#111', textSecondary: '#666', separator: '#eee' }) },
      '@/components/gmail-thread-review': { GmailOverview: 'GmailOverview', GmailThreadRow: 'GmailThreadRow' },
      '@/components/calendar-mutation-review': { CalendarMutationCard: 'CalendarMutationCard' },
      '@/components/apple-contact-mutation-review': { AppleContactMutationCard: 'AppleContactMutationCard' },
      '@/components/contact-mutation-review': { ContactMutationCard: 'ContactMutationCard' },
      '@/components/email-rich-editor': { EmailFormatToolbar: 'EmailFormatToolbar' },
      '@/components/gmail-email-compose-review': { GmailEmailComposeCard: 'GmailEmailComposeCard' },
      '@/components/gmail-settings-review': { GmailSettingsCard: 'GmailSettingsCard' },
      '@/components/slack-read-review': { SlackMarkReadCard: 'SlackMarkReadCard' },
      '@/components/slack-send-review': { SlackSendMessageCard: 'SlackSendMessageCard' },
      '@/components/text-message-review': { TextMessageCard: 'TextMessageCard' },
      '@/components/undo-bar': { UndoBar: 'UndoBar', useDecidedRequestIds: () => new Set() },
      '@/components/status-pill': { StatusPill: 'StatusPill' },
      '@/lib/api': {
        approveMutationRequest: async () => req,
        cancelMutationRequest: async () => req,
        undoMutationRequest: async () => req,
        getMutationRequest: async () => req,
        listMutationRequests: async () => [req],
        rejectMutationRequest: async () => req,
        removeMutation: async () => req.mutations[0],
        restoreMutation: async () => req.mutations[0],
        updateEmailMutation: async (_cfg, reqId, mutId, input) => {
          updatedCalls.push({ reqId, mutId, input });
          return req.mutations[0];
        },
        updateSlackMessageMutation: async () => req.mutations[0],
        updateTextMessageMutation: async () => req.mutations[0],
      },
      '@/lib/format': { formatWhen: () => '', pretty: (v) => JSON.stringify(v) },
      '@/lib/mutation-review': mutationReview,
      '@/lib/mutation-cache': {
        peekMutationRequest: () => req,
        peekReviewQueue: () => [req],
        rememberMutationRequest: (r) => r,
        rememberMutationRequests: (rs) => rs,
      },
      '@/lib/review-queue': { nextPendingRequestId: () => null, pendingReviewCount: () => 1 },
      '@/lib/review-paint': { markReviewOpen: () => {}, takeReviewPaint: () => null },
      '@/lib/review-prefetch': { prefetchMutationRequest: () => {}, readMutationRequest: async () => req },
      '@/features/feed/engagement': { connectFeedLogger: () => {}, recordUi: () => {} },
      '@/lib/undo-decision': {
        decisions: {
          hold: (entry) => heldDecisions.push(entry),
          decidedRequestIds: () => new Set(),
        },
      },
      '@/lib/session': { useConfig: () => ({ baseUrl: 'http://localhost', token: 't' }) },
      '@/lib/haptics': { confirm: () => {} },
    };
    const source = ts.transpileModule(
      readFileSync(new URL('../app/mutations/[id].tsx', import.meta.url), 'utf8'),
      { compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX } },
    ).outputText;
    const exports = {};
    vm.runInNewContext(source, {
      exports,
      require: (id) => modules[id] ?? require(id),
      setTimeout,
      clearTimeout,
      requestAnimationFrame: () => 0,
      cancelAnimationFrame: () => {},
    });
    hooks.beginRender();
    const screen = exports.default();
    // Screen returns <MutationReview key={id} id={id} />; invoke its function type directly.
    const tree = screen.type(screen.props);
    const card = findElements(tree, (el) => typeof el.type === 'function' && el.type.name === 'MutationCard')[0];
    assert.ok(card, 'expected MutationCard in MutationReview tree');
    const buttons = findElements(tree, (el) => el.type === 'Pressable' && el.props?.accessibilityRole === 'button');
    const approveBtn = buttons.at(-1);
    assert.ok(approveBtn, 'expected Approve Pressable button');
    return { card, approveBtn, updatedCalls, heldDecisions };
  }

  // Case 1: User typed a character and tapped Approve before onPendingChange ran.
  // Approve MUST flush the live editor HTML and save it before holding the decision.
  {
    const { card, approveBtn, updatedCalls, heldDecisions } = mountScreen();
    const flushedInput = {
      delivery_mode: 'send',
      editor_format: 'html',
      body_edited: true,
      message: { to: ['alex@example.com'], subject: 'Hello', body_html: '<p>couldX</p>', body_text: 'couldX' },
    };
    card.props.onRegisterEmailFlush(async () => flushedInput);
    await approveBtn.props.onPress();
    assert.equal(updatedCalls.length, 1);
    assert.equal(updatedCalls[0].input.message.body_html, '<p>couldX</p>');
    assert.equal(heldDecisions.length, 1);
  }

  // Case 2: Pending state held a stale edit, but user backspaced back to baseline
  // right before tapping Approve -> flush returns null -> NO updateEmailMutation call.
  {
    const { card, approveBtn, updatedCalls, heldDecisions } = mountScreen();
    card.props.onPendingChange({
      kind: 'email',
      input: {
        delivery_mode: 'send',
        editor_format: 'html',
        body_edited: true,
        message: { to: ['alex@example.com'], subject: 'Hello', body_html: '<p>stale</p>', body_text: 'stale' },
      },
    });
    card.props.onRegisterEmailFlush(async () => null);
    await approveBtn.props.onPress();
    assert.equal(updatedCalls.length, 0);
    assert.equal(heldDecisions.length, 1);
  }
});
