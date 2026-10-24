import { appPathForWebUrl } from '@/lib/app-links';

// A PDW web link opened on the phone (Android App Links) lands on the same
// screen in the app; every other URL (pdw://, the dev client) passes through.
export function redirectSystemPath({ path }: { path: string; initial: boolean }): string {
  return appPathForWebUrl(path) ?? path;
}
