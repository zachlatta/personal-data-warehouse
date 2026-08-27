import * as SecureStore from 'expo-secure-store';

// The app authenticates exactly like the pdw CLI: a static bearer of the form
// "<client_name>:<PDW_SECRET_TOKEN>" against the app's base URL. The token is
// the app secret, so it lives in the iOS Keychain (SecureStore), never in
// AsyncStorage or app state that could be serialized.
export type AppConfig = {
  baseUrl: string;
  token: string;
  clientName: string;
};

const KEYS = {
  baseUrl: 'pdw.base_url',
  token: 'pdw.secret_token',
  clientName: 'pdw.client_name',
} as const;

export const DEFAULT_BASE_URL = 'https://personal-data-warehouse.zachlatta.com';

export function normalizeBaseUrl(raw: string): string {
  let url = raw.trim();
  if (url === '') return '';
  if (!/^https?:\/\//i.test(url)) url = `https://${url}`;
  return url.replace(/\/+$/, '');
}

export function normalizeClientName(raw: string): string {
  // Mirrors auth.ValidateClientName on the server: lowercase [a-z0-9_-], <= 64.
  const cleaned = raw
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 64);
  return cleaned === '' ? 'ios' : cleaned;
}

export async function loadConfig(): Promise<AppConfig | null> {
  const [baseUrl, token, clientName] = await Promise.all([
    SecureStore.getItemAsync(KEYS.baseUrl),
    SecureStore.getItemAsync(KEYS.token),
    SecureStore.getItemAsync(KEYS.clientName),
  ]);
  if (!baseUrl || !token) return null;
  return { baseUrl, token, clientName: clientName ?? 'ios' };
}

export async function saveConfig(config: AppConfig): Promise<void> {
  await Promise.all([
    SecureStore.setItemAsync(KEYS.baseUrl, config.baseUrl),
    SecureStore.setItemAsync(KEYS.token, config.token),
    SecureStore.setItemAsync(KEYS.clientName, config.clientName),
  ]);
}

export async function clearConfig(): Promise<void> {
  await Promise.all(Object.values(KEYS).map((key) => SecureStore.deleteItemAsync(key)));
}
