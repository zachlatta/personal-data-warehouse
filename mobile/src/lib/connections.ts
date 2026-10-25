// Copy and parsing for the MCP connections screen, kept free of React Native
// so it is testable. Wording follows the web /connections page.

export const CONNECTION_NAME_PATTERN = /^[a-z]([a-z0-9_]{0,22}[a-z0-9])?$/;

export function validConnectionName(name: string): boolean {
  return CONNECTION_NAME_PATTERN.test(name) && !name.includes('__');
}

export function clientNames(value: string): string[] {
  return [...new Set(value.split(',').map((name) => name.trim()).filter(Boolean))];
}

export function scopeList(value: string): string[] {
  return value.trim().split(/\s+/).filter(Boolean);
}

export type ConnectionTone = 'good' | 'neutral' | 'muted' | 'bad';

export function connectionState(c: { enabled: boolean; status: string }): { label: string; tone: ConnectionTone } {
  if (!c.enabled) return { label: 'Disabled', tone: 'muted' };
  const label = c.status.replaceAll('_', ' ');
  const pretty = label.charAt(0).toUpperCase() + label.slice(1);
  if (c.status === 'connected') return { label: pretty, tone: 'good' };
  if (/failed|denied/.test(c.status)) return { label: pretty, tone: 'bad' };
  return { label: pretty, tone: 'neutral' };
}

export function accessLabel(c: { all_clients: boolean; clients: string[] | null }): string {
  if (c.all_clients) return 'All PDW clients';
  const names = c.clients ?? [];
  return names.length ? names.join(', ') : 'Not shared';
}

// The OAuth callback sends the browser to pdw://connections?auth=<result>.
export function authResultFromUrl(url: string): string | null {
  const match = /[?&]auth=([a-z_]+)/.exec(url);
  return match ? match[1] : null;
}

export function authMessage(result: string): string {
  switch (result) {
    case 'connected':
      return 'Connected. Tools are ready. Some MCP apps need a reconnect to discover newly added tools.';
    case 'discovery_failed':
      return 'Authorization succeeded, but tool discovery failed. Tap Refresh tools to retry.';
    default:
      return 'Authorization failed or expired. Tap Connect to try again.';
  }
}
