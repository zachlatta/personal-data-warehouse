// The recent searches file (recent-searches.ts has the list rules).
import { File, Paths } from 'expo-file-system';

import { parseRecent } from './recent-searches';

function file(): File {
  return new File(Paths.document, 'pdw-recent-searches.json');
}

export function readRecentSearches(): string[] {
  try {
    const f = file();
    return f.exists ? parseRecent(f.textSync()) : [];
  } catch {
    return [];
  }
}

export function writeRecentSearches(list: string[]): void {
  try {
    file().write(JSON.stringify(list));
  } catch {
    // Losing the list only costs typing a search again.
  }
}

export function forgetRecentSearches(): void {
  try {
    const f = file();
    if (f.exists) f.delete();
  } catch {
    // Nothing to remove.
  }
}
