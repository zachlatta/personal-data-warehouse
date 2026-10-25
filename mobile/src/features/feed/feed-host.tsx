import { useFocusEffect as useRouterFocusEffect, useNavigation, useRouter } from 'expo-router';
import { createContext, useContext, useEffect, useLayoutEffect, useState, useSyncExternalStore, type EffectCallback, type ReactNode } from 'react';
import { DeviceEventEmitter, Linking } from 'react-native';

// Where the Feed is drawn: the app's Feed tab (expo-router), or the launcher's
// left page (left-screen.tsx), which has no navigator. The screen takes the
// few navigation things it needs from here. A host never changes within a
// tree, so each hook below always calls the same hooks.

// Whether the left page is on screen. LeftScreenActivity sends this on resume
// and pause, before the app-wide AppState change, so the tab does not start a
// feed session while Zach is looking at the left page.
let leftVisible = false;
const listeners = new Set<() => void>();
function setLeftVisible(visible: boolean) {
  if (visible === leftVisible) return;
  leftVisible = visible;
  for (const l of listeners) l();
}
DeviceEventEmitter.addListener('pdwLeftScreenVisible', (visible: boolean) => setLeftVisible(!!visible));
export function markLeftScreenMounted() {
  setLeftVisible(true);
}
function subscribe(l: () => void) {
  listeners.add(l);
  return () => listeners.delete(l);
}
export function useLeftScreenVisible(): boolean {
  return useSyncExternalStore(subscribe, () => leftVisible);
}

export type FeedHost = {
  embedded: boolean;
  // Is this surface the one in front (for AppState handling)?
  inFront: () => boolean;
  useFocusEffect: (effect: EffectCallback) => void;
  // Puts a button at the header's right; returns a header to draw, if the
  // host has none of its own.
  useHeaderRight: (render: () => ReactNode, deps: unknown[]) => ReactNode;
  useTabPress: (onPress: (isFocused: () => boolean) => void) => void;
  useOpenItem: () => (id: string, position: number) => void;
};

type TabNavigation = {
  addListener(type: 'tabPress', cb: () => void): () => void;
  isFocused(): boolean;
  setOptions(options: { headerRight?: () => ReactNode }): void;
};

export const routerHost: FeedHost = {
  embedded: false,
  inFront: () => !leftVisible,
  useFocusEffect: useRouterFocusEffect,
  useHeaderRight(render, deps) {
    const navigation = useNavigation<TabNavigation>();
    // eslint-disable-next-line react-hooks/exhaustive-deps
    useLayoutEffect(() => navigation.setOptions({ headerRight: render }), [navigation, ...deps]);
    return null;
  },
  useTabPress(onPress) {
    const navigation = useNavigation<TabNavigation>();
    useEffect(() => navigation.addListener('tabPress', () => onPress(() => navigation.isFocused())), [navigation, onPress]);
  },
  useOpenItem() {
    const router = useRouter();
    return (id, position) => router.push({ pathname: '/feed/[id]', params: { id, position: String(position) } });
  },
};

export function makeEmbeddedHost(header: (right: ReactNode) => ReactNode): FeedHost {
  return {
    embedded: true,
    inFront: () => leftVisible,
    // Focused while the left page is on screen.
    useFocusEffect(effect) {
      const visible = useLeftScreenVisible();
      useEffect(() => (visible ? effect() : undefined), [visible, effect]);
    },
    useHeaderRight(render, deps) {
      const [node, setNode] = useState<ReactNode>(null);
      // eslint-disable-next-line react-hooks/exhaustive-deps
      useLayoutEffect(() => setNode(render()), deps);
      return header(node);
    },
    useTabPress() {},
    // A card opens in the app, in its own task (LeftScreenActivity adds
    // FLAG_ACTIVITY_NEW_TASK), so the left page stays as it was.
    useOpenItem() {
      return (id, position) => void Linking.openURL(`pdw://feed/${encodeURIComponent(id)}?position=${position}`);
    },
  };
}

const FeedHostContext = createContext<FeedHost>(routerHost);
export const FeedHostProvider = FeedHostContext.Provider;
export function useFeedHost(): FeedHost {
  return useContext(FeedHostContext);
}
