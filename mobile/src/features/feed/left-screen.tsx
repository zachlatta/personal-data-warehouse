import { useEffect, useMemo, type ReactNode } from 'react';
import { StyleSheet, View } from 'react-native';
import { GestureHandlerRootView } from 'react-native-gesture-handler';
import { SafeAreaProvider, useSafeAreaInsets } from 'react-native-safe-area-context';

import { ThemedText } from '@/components/themed-text';
import { ThemedView } from '@/components/themed-view';
import { Spacing } from '@/constants/theme';
import { SessionProvider, useSession } from '@/lib/session';

import FeedScreen from './feed-screen';
import { FeedHostProvider, makeEmbeddedHost, markLeftScreenMounted } from './feed-host';
import { TrackedBrowserHost } from './tracked-browser';

// The launcher's left page: swipe right on the first home page and the Feed
// slides in, as Google Discover did; swipe left and the home page comes back.
// Drawn by LeftScreenActivity (plugins/with-left-screen.js), registered as the
// `leftscreen` component in index.js. The launcher runs the swipes; this is
// only the Feed, with its own header since there is no navigator here.

function Header({ right }: { right: ReactNode }) {
  const insets = useSafeAreaInsets();
  return (
    <View style={[styles.header, { paddingTop: insets.top + Spacing.two }]}>
      <ThemedText type="subtitle" style={styles.title}>
        Feed
      </ThemedText>
      {right}
    </View>
  );
}

function Body() {
  const { ready, config } = useSession();
  const host = useMemo(() => makeEmbeddedHost((right) => <Header right={right} />), []);
  if (!ready) return <ThemedView style={styles.fill} />;
  if (!config) {
    return (
      <ThemedView style={[styles.fill, styles.center]}>
        <ThemedText>Open PDW and sign in to see your Feed here.</ThemedText>
      </ThemedView>
    );
  }
  return (
    <FeedHostProvider value={host}>
      <FeedScreen />
    </FeedHostProvider>
  );
}

export default function LeftScreen() {
  useEffect(() => markLeftScreenMounted(), []);
  return (
    <GestureHandlerRootView style={styles.fill}>
      <SafeAreaProvider>
        <SessionProvider>
          <Body />
          <TrackedBrowserHost />
        </SessionProvider>
      </SafeAreaProvider>
    </GestureHandlerRootView>
  );
}

const styles = StyleSheet.create({
  fill: { flex: 1 },
  center: { alignItems: 'center', justifyContent: 'center', padding: Spacing.four },
  header: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', paddingLeft: Spacing.three, paddingBottom: Spacing.two },
  // No size override: a smaller fontSize on this text clipped it to "Fee" on Android.
  title: { flexShrink: 0, paddingRight: 4 },
});
