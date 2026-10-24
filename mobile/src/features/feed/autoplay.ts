// Which feed video plays as he scrolls, the way X and Instagram do it: the
// card with a video that is most on screen, once at least AUTOPLAY_FROM of
// it is, starts (muted); it keeps playing until it is mostly gone (below
// STOP_BELOW) or another video card takes the focus. A tapped video is left
// alone until its card scrolls away. Pure, so it has node tests.

export const AUTOPLAY_FROM = 0.6;
export const STOP_BELOW = 0.25;

export type AutoplayStep = {
  // The video card that has the focus now, if any.
  focused: string | null;
  // The player key to start, null to stop the playing one, undefined to leave it.
  play?: string | null;
};

/**
 * fractions: how much of each laid-out card is on screen (viewability.ts
 * visibleFraction), top-down. keys: item id -> player key, for the cards
 * with a video. focused: the last step's focus. playing: the key playing now.
 */
export function autoplayStep(
  fractions: Map<string, number>,
  keys: Map<string, string>,
  focused: string | null,
  playing: string | null,
): AutoplayStep {
  const fraction = (id: string) => fractions.get(id) ?? 0;
  let next: string | null = focused && keys.has(focused) && fraction(focused) >= AUTOPLAY_FROM ? focused : null;
  if (!next) {
    let best = 0;
    for (const [id, f] of fractions) {
      if (!keys.has(id) || f < AUTOPLAY_FROM) continue;
      // Ties go to the higher card.
      if (f > best) {
        best = f;
        next = id;
      }
    }
  }
  if (next && next !== focused) return { focused: next, play: keys.get(next) };
  if (playing) {
    for (const [id, key] of keys) {
      if (key === playing && fraction(id) < STOP_BELOW) return { focused: next, play: null };
    }
  }
  return { focused: next };
}
