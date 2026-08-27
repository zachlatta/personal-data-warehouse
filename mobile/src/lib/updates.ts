import * as Updates from 'expo-updates';

export type UpdateState =
  | { state: 'disabled' }
  | { state: 'checking' }
  | { state: 'downloading' }
  | { state: 'current' }
  | { state: 'error'; message: string };

// Fetch and apply a pending OTA update right away, instead of expo-updates'
// default of applying it on the launch after the one that downloaded it.
// Returns 'current' when there is nothing newer; reloads the app otherwise.
export async function applyUpdateNow(onState: (state: UpdateState) => void): Promise<void> {
  if (!Updates.isEnabled || __DEV__) {
    onState({ state: 'disabled' });
    return;
  }
  try {
    onState({ state: 'checking' });
    const check = await Updates.checkForUpdateAsync();
    if (!check.isAvailable) {
      onState({ state: 'current' });
      return;
    }
    onState({ state: 'downloading' });
    const fetched = await Updates.fetchUpdateAsync();
    if (fetched.isNew) {
      await Updates.reloadAsync();
      return;
    }
    onState({ state: 'current' });
  } catch (error) {
    onState({ state: 'error', message: error instanceof Error ? error.message : String(error) });
  }
}

export function describeInstalledUpdate(): string {
  if (!Updates.isEnabled) return 'OTA updates disabled (dev build)';
  const id = Updates.updateId ? Updates.updateId.slice(0, 8) : 'embedded';
  const when = Updates.createdAt ? ` · ${Updates.createdAt.toLocaleString()}` : '';
  return `${id} · channel ${Updates.channel ?? 'none'} · runtime ${Updates.runtimeVersion ?? '?'}${when}`;
}
