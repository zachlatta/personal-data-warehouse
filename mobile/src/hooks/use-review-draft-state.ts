import { useState, type SetStateAction } from 'react';

import { reviewDrafts } from '@/lib/review-drafts';

export function useReviewDraftState<T>(key: string, baseline: string, initial: T, enabled = true): [T, (next: SetStateAction<T>) => void] {
  const [snapshot, setSnapshot] = useState(() => ({ key, baseline, enabled, value: enabled ? reviewDrafts.read(key, baseline, initial) : initial }));
  let value = snapshot.value;
  if (snapshot.key !== key || snapshot.baseline !== baseline || snapshot.enabled !== enabled) {
    value = enabled ? reviewDrafts.read(key, baseline, initial) : initial;
    setSnapshot({ key, baseline, enabled, value });
  }
  if (!enabled) reviewDrafts.remove(key);
  const setValue = (next: SetStateAction<T>) => {
    const updated = typeof next === 'function' ? (next as (previous: T) => T)(value) : next;
    if (enabled) reviewDrafts.write(key, baseline, updated);
    setSnapshot({ key, baseline, enabled, value: updated });
  };
  return [value, setValue];
}
