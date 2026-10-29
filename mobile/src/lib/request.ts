// One bounded transport for every app screen. The deadline includes reading
// the body: receiving headers alone must not leave a spinner up indefinitely.
export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export async function requestJson<T>(
  url: string,
  init: RequestInit = {},
  timeoutMs = (init.method ?? 'GET').toUpperCase() === 'GET' ? 8_000 : 20_000,
  transport: typeof fetch = fetch,
): Promise<T> {
  const controller = new AbortController();
  const abort = () => controller.abort();
  if (init.signal?.aborted) abort();
  else init.signal?.addEventListener('abort', abort, { once: true });
  let timer: ReturnType<typeof setTimeout> | undefined;
  const read = async () => {
    const response = await transport(url, { ...init, signal: controller.signal });
    const text = await response.text();
    let parsed: unknown;
    try { parsed = text ? JSON.parse(text) : null; } catch { parsed = undefined; }
    if (!response.ok) {
      const message = parsed && typeof parsed === 'object' && 'error' in parsed && typeof parsed.error === 'string'
        ? parsed.error
        : response.status === 401 ? 'Your sign-in has expired. Open Settings to reconnect.'
          : response.status >= 500 ? 'PDW is unavailable. Please try again shortly.' : `Request failed (HTTP ${response.status}).`;
      throw new ApiError(response.status, message);
    }
    if (parsed === undefined) throw new ApiError(502, 'PDW returned an unreadable response. Please try again.');
    return parsed as T;
  };
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      reject(new ApiError(0, (init.method ?? 'GET').toUpperCase() === 'GET'
        ? 'Connection timed out. Please try again.'
        : 'Connection timed out. Check the current status before trying again; the change may have reached PDW.'));
      controller.abort();
    }, timeoutMs);
  });
  try {
    // No automatic retries, especially for a write with an uncertain outcome.
    return await Promise.race([read(), timeout]);
  } finally {
    clearTimeout(timer);
    init.signal?.removeEventListener('abort', abort);
  }
}
