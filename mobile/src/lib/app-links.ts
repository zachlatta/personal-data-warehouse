// Web app URLs that the phone app opens itself. Android hands it any
// https://<host>/<prefix>… link (the intent filters in app.json, verified by
// the server's /.well-known/assetlinks.json); this maps the web path to the
// matching screen. Keep the prefixes, app.json and the server's webapp.Routes
// in step. /connections stays in the browser: the app has no screen for it.
export const APP_LINK_HOSTS = ['personal-data-warehouse.zachlatta.com'];
export const APP_LINK_PREFIXES = ['/timeline', '/mutation-review', '/search', '/notifications'];

const reencode = (segment: string) => {
  try {
    return encodeURIComponent(decodeURIComponent(segment));
  } catch {
    return null;
  }
};

// The in-app path for a PDW web URL, or null when the URL is not one the app
// owns (another host, scheme or path), so the caller can leave it alone.
export function appPathForWebUrl(raw: string): string | null {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    return null;
  }
  if (url.protocol !== 'https:' || !APP_LINK_HOSTS.includes(url.hostname.toLowerCase())) return null;
  const path = url.pathname.replace(/\/+$/, '') || '/';

  if (path === '/timeline') return '/';
  const item = /^\/timeline\/([^/]+)\/(.+)$/.exec(path);
  if (item) {
    const adapter = reencode(item[1]);
    const eventId = reencode(item[2]);
    return adapter && eventId ? `/timeline/${adapter}/${eventId}` : '/';
  }

  if (path === '/mutation-review' || path === '/mutation-review/requests') return '/mutations';
  const request = /^\/mutation-review\/requests\/([^/]+)$/.exec(path);
  if (request) {
    const id = reencode(request[1]);
    return id ? `/mutations/${id}` : '/mutations';
  }

  if (path === '/search') return '/search';
  if (path === '/notifications') return '/notifications';
  return null;
}
