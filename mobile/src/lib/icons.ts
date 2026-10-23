// Every icon the app draws, as its SF Symbol (iOS) and the Material Symbol
// Android shows in its place. SymbolView renders nothing on Android for a
// bare SF name, so a glyph not listed here is a glyph Android users never see.
// The Material font has no filled variants: a filled SF state (heart.fill)
// reads through its tint colour on Android instead.
export const MATERIAL_ICON = {
  'arrow.2.squarepath': 'repeat',
  'arrow.up': 'arrow_upward',
  'arrow.up.right.square': 'open_in_new',
  'arrowtriangle.up.fill': 'arrow_drop_up',
  bookmark: 'bookmark',
  'bookmark.fill': 'bookmark_added',
  'bookmark.slash': 'bookmark_remove',
  'bubble.left': 'chat_bubble',
  'bubble.right': 'chat_bubble',
  'chart.bar': 'bar_chart',
  'checkmark.seal': 'verified',
  'checkmark.seal.fill': 'verified',
  'chevron.right': 'chevron_right',
  clock: 'schedule',
  ellipsis: 'more_horiz',
  'eye.slash.fill': 'visibility_off',
  gearshape: 'settings',
  heart: 'favorite',
  'heart.fill': 'favorite',
  link: 'link',
  newspaper: 'newspaper',
  paperplane: 'send',
  'play.fill': 'play_arrow',
  safari: 'open_in_browser',
  sparkles: 'auto_awesome',
  'square.and.arrow.up': 'share',
  'text.bubble': 'comment',
  xmark: 'close',
} as const;

export type IconName = keyof typeof MATERIAL_ICON;
