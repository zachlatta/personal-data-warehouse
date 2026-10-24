import { Platform } from 'react-native';

// A Superhuman-style palette: a warm charcoal dark and an off-white light.
// Text is white or black at fixed opacities rather than greys, so it sits on
// any surface the same way.
export const Colors = {
  light: {
    text: 'rgba(0,0,0,0.92)',
    background: '#FCFAF7',
    backgroundElement: '#F1EEE9',
    backgroundSelected: '#E6E2DC',
    textSecondary: 'rgba(0,0,0,0.5)',
    separator: 'rgba(0,0,0,0.07)',
    surface: '#FFFFFF',
    surfaceBorder: 'rgba(0,0,0,0.08)',
    accent: '#1173A8',
    unread: '#36A0DA',
  },
  dark: {
    text: 'rgba(255,255,255,0.9)',
    background: '#292827',
    backgroundElement: '#33312F',
    backgroundSelected: '#3D3B39',
    textSecondary: 'rgba(255,255,255,0.5)',
    separator: 'rgba(255,255,255,0.07)',
    surface: '#363433',
    surfaceBorder: 'rgba(255,255,255,0.08)',
    accent: '#72C0EA',
    unread: '#72C0EA',
  },
} as const;

// Swipe and state colours from the same palette: done is teal, remind pink,
// destructive coral, star amber.
export const Palette = {
  done: '#4FAFA3',
  doneActive: '#6EC6BC',
  remind: '#D180A6',
  unread: '#67B4E0',
  destructive: '#F2838D',
  star: '#E0A05C',
  organize: '#BBA6E5',
  brand: '#714CB6',
} as const;

export type ThemeColor = keyof typeof Colors.light & keyof typeof Colors.dark;

export const Fonts = Platform.select({
  ios: {
    /** iOS `UIFontDescriptorSystemDesignDefault` */
    sans: 'system-ui',
    /** iOS `UIFontDescriptorSystemDesignSerif` */
    serif: 'ui-serif',
    /** iOS `UIFontDescriptorSystemDesignRounded` */
    rounded: 'ui-rounded',
    /** iOS `UIFontDescriptorSystemDesignMonospaced` */
    mono: 'ui-monospace',
  },
  default: {
    sans: 'normal',
    serif: 'serif',
    rounded: 'normal',
    mono: 'monospace',
  },
  web: {
    sans: 'var(--font-display)',
    serif: 'var(--font-serif)',
    rounded: 'var(--font-rounded)',
    mono: 'var(--font-mono)',
  },
});

export const Spacing = {
  half: 2,
  one: 4,
  two: 8,
  three: 16,
  four: 24,
  five: 32,
  six: 64,
} as const;

export const BottomTabInset = Platform.select({ ios: 50, android: 80 }) ?? 0;
export const MaxContentWidth = 800;
