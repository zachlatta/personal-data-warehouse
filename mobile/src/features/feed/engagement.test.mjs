import test from 'node:test';
import assert from 'node:assert/strict';
import { registerHooks } from 'node:module';
import { FeedSessionLifecycle, FeedSessionCoordinator } from './session-lifecycle.ts';

// Replace only native IO. Exercise the production logger, queue and session state.
const hooks = registerHooks({
  resolve(specifier, context, next) {
    if (['expo-constants', 'expo-device', 'expo-updates', 'react-native', 'expo-file-system'].includes(specifier) || specifier === './api') {
      return { url: `native:${specifier}`, shortCircuit: true };
    }
    if (context.parentURL?.endsWith('/engagement.ts') && specifier.startsWith('./')) return next(specifier + '.ts', context);
    return next(specifier, context);
  },
  load(url, context, next) {
    const source = {
      'native:expo-constants': 'export default {expoConfig:{version:"test"}}',
      'native:expo-device': 'export const modelName="test", osName="test", osVersion="test";',
      'native:expo-updates': 'export const updateId="test", channel="test";',
      'native:react-native': 'export const AppState={currentState:"active",addEventListener(){}}; export const Appearance={getColorScheme(){return "light"}};',
      'native:expo-file-system': 'export const Paths={document:"test"}; export class File {exists=false; write(){} }',
      'native:./api': 'export async function postFeedEvents() {throw Error("offline fixture")}',
    }[url];
    return source ? {format:'module', source, shortCircuit:true} : next(url, context);
  },
});
const logger = await import('./engagement.ts');
hooks.deregister();
for (const host of ['tab', 'embedded']) {
  test(`${host}: real logger stamps impression, detail, browser and close with one id`, () => {
    const lifecycle = new FeedSessionLifecycle();
    lifecycle.focus();
    logger.startFeedSession(1, 'focus');
    logger.recordFeedEvent('impression', {item_id:host});
    lifecycle.openDetail();
    if (lifecycle.blur()) logger.endFeedSession('blur');
    for (const type of ['open', 'web', 'close']) logger.recordFeedEvent(type, {item_id:host});
    const events = logger.pendingFeedEvents().filter(e => e.item_id === host);
    assert.equal(events.length, 4);
    assert.ok(events[0].session_id);
    assert.equal(new Set(events.map(e => e.session_id)).size, 1);
    logger.endFeedSession('background');
  });
}

test('launcher detail survives the previously focused tab blurring', () => {
  const coordinator = new FeedSessionCoordinator();
  const tab = new FeedSessionLifecycle(coordinator);
  const launcher = new FeedSessionLifecycle(coordinator);
  tab.focus();
  launcher.focus();
  logger.startFeedSession(1, 'focus');
  logger.recordFeedEvent('impression', {item_id:'cross-host'});
  launcher.openDetail();
  if (launcher.blur()) logger.endFeedSession('blur');
  if (tab.blur()) logger.endFeedSession('blur');
  assert.equal(tab.appState('active'), null);
  assert.equal(launcher.appState('background'), null);
  launcher.appState('active');
  for (const type of ['open', 'web', 'close']) logger.recordFeedEvent(type, {item_id:'cross-host'});
  const events = logger.pendingFeedEvents().filter(e => e.item_id === 'cross-host');
  assert.equal(events.length, 4);
  assert.ok(events[0].session_id);
  assert.ok(events.every(e => e.session_id === events[0].session_id));
  assert.equal(launcher.closeDetail(false), true);
  logger.endFeedSession('blur');
});

test('focused tab resumes after launcher hides without a router focus change', () => {
  const coordinator = new FeedSessionCoordinator();
  const tab = new FeedSessionLifecycle(coordinator);
  const launcher = new FeedSessionLifecycle(coordinator);
  tab.focus();
  logger.startFeedSession(1, 'focus');
  if (tab.appState('background') === 'end') logger.endFeedSession('background');
  launcher.focus();
  logger.startFeedSession(1, 'focus');
  if (launcher.blur()) logger.endFeedSession('blur');
  if (tab.appState('active', true) === 'start') logger.startFeedSession(1, 'foreground');
  logger.recordFeedEvent('impression', {item_id:'launcher-return'});
  const event = logger.pendingFeedEvents().find(e => e.item_id === 'launcher-return');
  assert.ok(event.session_id);
  assert.equal(tab.owns, true);
  logger.endFeedSession('background');
});
