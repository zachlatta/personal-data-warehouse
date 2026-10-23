import { SymbolView, type AndroidSymbol, type SFSymbol, type SymbolViewProps } from 'expo-symbols';

import { MATERIAL_ICON, type IconName } from '@/lib/icons';

// The one place the app draws a symbol: the SF Symbol on iOS, its Material
// counterpart on Android (src/lib/icons.ts).
export function Icon({ name, ...props }: Omit<SymbolViewProps, 'name'> & { name: IconName }) {
  const material = MATERIAL_ICON[name] as AndroidSymbol;
  return <SymbolView {...props} name={{ ios: name as SFSymbol, android: material, web: material }} />;
}
