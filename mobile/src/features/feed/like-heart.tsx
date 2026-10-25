import { useEffect, useRef } from 'react';
import { Platform, StyleSheet, View } from 'react-native';
import Animated, {
  Easing,
  useAnimatedStyle,
  useReducedMotion,
  useSharedValue,
  withDelay,
  withSequence,
  withSpring,
  withTiming,
  type SharedValue,
} from 'react-native-reanimated';

import { Icon } from '@/components/icon';

import { onLiked } from './liked';

// Liking should feel like it landed (Zach, voice memo 2026-10-25: "give me an
// animation, vibrate my phone, make it like really clear I've done it"). The
// heart squashes and springs back past full size, a ring and a spray of dots
// burst out of it, and the card shows a big heart for a moment (CardLikeBurst).
// It plays only on a like he just made (liked.ts onLiked), never when a card
// loads already liked or is recycled for another card. Reduce Motion skips it;
// the buzz still comes.

const DOTS = 6;
const DOT_COLORS = ['#F91880', '#FFAD1F', '#F91880', '#794BC4', '#F91880', '#17BF63'];

/** A filled heart. Android's Material font has no filled glyph (icons.ts), so
 * there it is drawn: a square turned 45 degrees with a circle on each upper
 * edge, sized to the outline glyph it replaces. */
function FilledHeart({ size, color }: { size: number; color: string }) {
  if (Platform.OS !== 'android') return <Icon name="heart.fill" tintColor={color} size={size} />;
  const s = size * 0.48;
  const cx = size / 2;
  const cy = size / 2 + 0.073 * s;
  const off = s / (2 * Math.SQRT2);
  const lobe = { position: 'absolute' as const, width: s, height: s, borderRadius: s / 2, top: cy - off - s / 2, backgroundColor: color };
  return (
    <View style={{ width: size, height: size }}>
      <View style={{ position: 'absolute', width: s, height: s, left: cx - s / 2, top: cy - s / 2, backgroundColor: color, transform: [{ rotate: '45deg' }] }} />
      <View style={[lobe, { left: cx - off - s / 2 }]} />
      <View style={[lobe, { left: cx + off - s / 2 }]} />
    </View>
  );
}

/** Calls play() each time he likes this card. */
function useOnLiked(itemId: string | undefined, play: () => void) {
  const ref = useRef(play);
  useEffect(() => {
    ref.current = play;
  });
  useEffect(() => {
    if (!itemId) return;
    return onLiked((id) => {
      if (id === itemId) ref.current();
    });
  }, [itemId]);
}

function Dot({ i, size, burst }: { i: number; size: number; burst: SharedValue<number> }) {
  const angle = (i / DOTS) * Math.PI * 2 - Math.PI / 2;
  const reach = size * 1.05;
  const dot = Math.max(3, Math.round(size * 0.2));
  const style = useAnimatedStyle(() => {
    const p = burst.value;
    const r = reach * (0.45 + 0.55 * p);
    return {
      opacity: p > 0 && p < 1 ? 1 - p * p : 0,
      transform: [{ translateX: Math.cos(angle) * r }, { translateY: Math.sin(angle) * r }, { scale: 1 - 0.6 * p }],
    };
  });
  return (
    <Animated.View
      pointerEvents="none"
      style={[styles.dot, { width: dot, height: dot, borderRadius: dot / 2, marginLeft: -dot / 2, marginTop: -dot / 2, backgroundColor: DOT_COLORS[i % DOT_COLORS.length] }, style]}
    />
  );
}

/** The heart, filled when liked, that bursts when he likes the card. */
export function LikeHeart({
  itemId,
  liked,
  size,
  tint,
  likedTint,
}: {
  itemId?: string;
  liked: boolean;
  size: number;
  tint: string;
  likedTint: string;
}) {
  const reduced = useReducedMotion();
  const scale = useSharedValue(1);
  const burst = useSharedValue(0);
  useOnLiked(itemId, () => {
    if (reduced) return;
    scale.value = withSequence(withTiming(0.55, { duration: 80 }), withSpring(1, { damping: 7, stiffness: 320, mass: 0.6 }));
    burst.value = 0;
    burst.value = withTiming(1, { duration: 560, easing: Easing.out(Easing.cubic) });
  });
  const heart = useAnimatedStyle(() => ({ transform: [{ scale: scale.value }] }));
  const ring = useAnimatedStyle(() => {
    const p = burst.value;
    return {
      opacity: p > 0 && p < 1 ? 1 - p : 0,
      borderWidth: Math.max(0.5, size * 0.22 * (1 - p)),
      transform: [{ scale: 0.35 + 0.9 * p }],
    };
  });
  const ringSize = size * 1.9;
  return (
    <View style={{ width: size, height: size }}>
      <Animated.View
        pointerEvents="none"
        style={[styles.ring, { width: ringSize, height: ringSize, borderRadius: ringSize / 2, marginLeft: -ringSize / 2, marginTop: -ringSize / 2, borderColor: likedTint }, ring]}
      />
      {Array.from({ length: DOTS }, (_, i) => (
        <Dot key={i} i={i} size={size} burst={burst} />
      ))}
      <Animated.View style={heart}>
        {liked ? <FilledHeart size={size} color={likedTint} /> : <Icon name="heart" tintColor={tint} size={size} />}
      </Animated.View>
    </View>
  );
}

/** A big heart that pops up over the card he just liked, then fades. */
export function CardLikeBurst({ itemId, color }: { itemId: string; color: string }) {
  const reduced = useReducedMotion();
  const scale = useSharedValue(0);
  const opacity = useSharedValue(0);
  useOnLiked(itemId, () => {
    if (reduced) return;
    scale.value = 0.2;
    scale.value = withSequence(withSpring(1, { damping: 9, stiffness: 260, mass: 0.7 }), withDelay(260, withTiming(1.25, { duration: 220 })));
    opacity.value = withSequence(withTiming(1, { duration: 90 }), withDelay(520, withTiming(0, { duration: 220 })));
  });
  const style = useAnimatedStyle(() => ({ opacity: opacity.value, transform: [{ scale: scale.value }] }));
  return (
    <View pointerEvents="none" style={styles.overlay}>
      {/* Composited as one layer, so the drawn heart's overlapping pieces fade as one. */}
      <Animated.View needsOffscreenAlphaCompositing style={[styles.bigHeart, style]}>
        <FilledHeart size={88} color={color} />
      </Animated.View>
    </View>
  );
}

const styles = StyleSheet.create({
  ring: { position: 'absolute', left: '50%', top: '50%' },
  dot: { position: 'absolute', left: '50%', top: '50%' },
  overlay: { position: 'absolute', left: 0, right: 0, top: 0, bottom: 0, alignItems: 'center', justifyContent: 'center' },
  bigHeart: {
    shadowColor: '#000',
    shadowOpacity: 0.25,
    shadowRadius: 12,
    shadowOffset: { width: 0, height: 4 },
  },
});
