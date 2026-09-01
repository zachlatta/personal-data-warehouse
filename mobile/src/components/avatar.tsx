import { Image } from 'expo-image';
import { useState } from 'react';
import { StyleSheet, View, type StyleProp, type ImageStyle, type ViewStyle } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { useTheme } from '@/hooks/use-theme';

function initial(value: string): string {
  const match = /[\p{L}\p{N}]/u.exec(value.trim());
  return match ? match[0].toUpperCase() : '?';
}

// A person's face, when the source gave us one. Slack keeps a profile image
// for nearly every user, and a review of hundreds of conversations reads far
// faster with faces than with a column of letter circles — but the image is
// remote, so the letter stays the fallback for an unreachable URL, a bot, or
// a source with no avatar at all.
export function Avatar({
  name,
  url,
  size = 34,
  style,
  highlight = false,
}: {
  name: string;
  url?: string | null;
  size?: number;
  style?: StyleProp<ViewStyle & ImageStyle>;
  highlight?: boolean;
}) {
  const theme = useTheme();
  const [failed, setFailed] = useState(false);
  const frame = { width: size, height: size, borderRadius: size / 2 };
  if (url && !failed) {
    return (
      <Image
        accessibilityIgnoresInvertColors
        alt={name}
        source={{ uri: url }}
        onError={() => setFailed(true)}
        contentFit="cover"
        transition={120}
        style={[frame, { backgroundColor: theme.backgroundElement }, highlight && styles.highlight, style as StyleProp<ImageStyle>]}
      />
    );
  }
  return (
    <View
      style={[
        frame,
        styles.fallback,
        { backgroundColor: highlight ? '#D97706' : theme.backgroundElement },
        highlight && styles.highlight,
        style,
      ]}>
      <ThemedText type="smallBold" style={highlight ? styles.highlightText : undefined}>{initial(name)}</ThemedText>
    </View>
  );
}

const styles = StyleSheet.create({
  fallback: { alignItems: 'center', justifyContent: 'center' },
  highlight: { borderWidth: 2, borderColor: '#D97706' },
  highlightText: { color: '#FFFFFF' },
});
