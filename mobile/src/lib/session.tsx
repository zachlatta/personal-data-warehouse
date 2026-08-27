import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';

import { clearConfig, loadConfig, saveConfig, type AppConfig } from './config';
import { registerForPush, type PushStatus } from './push';

type SessionValue = {
  config: AppConfig | null;
  ready: boolean;
  push: PushStatus | null;
  signIn: (config: AppConfig) => Promise<void>;
  signOut: () => Promise<void>;
  refreshPush: () => Promise<void>;
};

const SessionContext = createContext<SessionValue | null>(null);

export function SessionProvider({ children }: { children: ReactNode }) {
  const [config, setConfig] = useState<AppConfig | null>(null);
  const [ready, setReady] = useState(false);
  const [push, setPush] = useState<PushStatus | null>(null);

  useEffect(() => {
    loadConfig()
      .then(setConfig)
      .finally(() => setReady(true));
  }, []);

  const refreshPush = useCallback(async () => {
    if (!config) return;
    setPush(await registerForPush(config));
  }, [config]);

  // Re-register on every launch: Expo tokens are stable but the server row is
  // cheap to refresh, and it is what reactivates a device the provider retired.
  useEffect(() => {
    if (!config) return;
    let cancelled = false;
    registerForPush(config).then((status) => {
      if (!cancelled) setPush(status);
    });
    return () => {
      cancelled = true;
    };
  }, [config]);

  const signIn = useCallback(async (next: AppConfig) => {
    await saveConfig(next);
    setConfig(next);
  }, []);

  const signOut = useCallback(async () => {
    await clearConfig();
    setConfig(null);
    setPush(null);
  }, []);

  const value = useMemo(() => ({ config, ready, push, signIn, signOut, refreshPush }), [config, ready, push, signIn, signOut, refreshPush]);
  return <SessionContext.Provider value={value}>{children}</SessionContext.Provider>;
}

export function useSession(): SessionValue {
  const value = useContext(SessionContext);
  if (!value) throw new Error('useSession must be used inside SessionProvider');
  return value;
}

// For screens that only render once signed in.
export function useConfig(): AppConfig {
  const { config } = useSession();
  if (!config) throw new Error('not signed in');
  return config;
}
