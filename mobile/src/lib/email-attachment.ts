import type { GmailEmailAttachment } from './mutation-review';

export function attachmentPreviewKind(contentType: string, platform: string): 'image' | 'text' | 'pdf' | 'external' {
  const mime = contentType.split(';')[0].trim().toLowerCase();
  if (['image/png', 'image/jpeg', 'image/gif', 'image/webp', 'image/heic'].includes(mime)) return 'image';
  if (mime === 'text/plain' || mime === 'text/csv' || mime === 'application/json') return 'text';
  if (mime === 'application/pdf' && platform === 'ios') return 'pdf';
  return 'external';
}

export function attachmentFilename(filename: string): string {
  const name = filename.split(/[\\/]/).pop()?.replace(/[\u0000-\u001f\u007f]/g, '').trim();
  return !name || name === '.' || name === '..' ? 'attachment' : name;
}

// Each viewer owns an isolated cache directory, never a shared filename. In
// particular, two proposal variants can attach different bytes under one name.
export function prepareAttachment(attachment: GmailEmailAttachment, storage: {
  write: (filename: string, base64: string) => string;
  remove: () => void;
}): { uri: string; dispose: () => void } {
  try {
    return { uri: storage.write(attachmentFilename(attachment.filename), attachment.data_base64), dispose: storage.remove };
  } catch (error) {
    storage.remove();
    throw error;
  }
}
