import test from 'node:test';
import assert from 'node:assert/strict';
import { registerHooks } from 'node:module';
import { FeedSessionLifecycle } from './session-lifecycle.ts';

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
