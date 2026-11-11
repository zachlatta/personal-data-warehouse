import { useEffect, useState } from 'react';
import { Alert, Pressable, StyleSheet, View } from 'react-native';

import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useTheme } from '@/hooks/use-theme';
import { getMutationPolicies, setAutoApprove, setMutationPolicyMode, type MutationPolicyOverview } from '@/lib/api';
import type { AppConfig } from '@/lib/config';

const MODES: { mode: 'off' | 'dry_run' | 'enforce'; label: string }[] = [
  { mode: 'off', label: 'Off' },
  { mode: 'dry_run', label: 'Dry run' },
  { mode: 'enforce', label: 'Enforce' },
];

// Automatic approvals, under the Mutations list: the one kill switch and each
// policy's mode. Turning anything off is one tap; turning it on asks first.
export function PolicySwitches({ config }: { config: AppConfig }) {
  const theme = useTheme();
  const [overview, setOverview] = useState<MutationPolicyOverview | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let active = true;
    void getMutationPolicies(config).then(
      (value) => {
        if (!active) return;
        setOverview(value);
        setError(null);
      },
      (e) => {
        if (active) setError(e instanceof Error ? e.message : String(e));
      },
    );
    return () => { active = false; };
  }, [config]);
  const run = (change: () => Promise<MutationPolicyOverview>, confirm?: string) => {
    const go = async () => {
      try {
        setOverview(await change());
        setError(null);
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      }
    };
    if (!confirm) return void go();
    Alert.alert(confirm, undefined, [
      { text: 'Cancel', style: 'cancel' },
      { text: 'Yes', onPress: () => void go() },
    ]);
  };
  if (!overview && !error) return null;
  return (
    <View style={styles.container}>
      <ThemedText type="smallBold" themeColor="textSecondary">Automatic approvals</ThemedText>
      {error ? <ThemedText type="small" style={styles.error}>{error}</ThemedText> : null}
      {overview ? (
        <>
          <View style={styles.row}>
            <ThemedText type="small" style={styles.grow}>
              {overview.auto_approve_forced_off ? 'Off on the deployment (PDW_MUTATION_AUTO_APPROVE=0)' : overview.auto_approve ? 'On' : 'Off: everything comes to you'}
            </ThemedText>
            {overview.auto_approve_forced_off ? null : (
              <Pressable
                accessibilityRole="button"
                onPress={() => run(() => setAutoApprove(config, !overview.auto_approve), overview.auto_approve ? undefined : 'Switch automatic approvals back on?')}
                style={[styles.button, { borderColor: theme.surfaceBorder }]}>
                <ThemedText type="small">{overview.auto_approve ? 'Switch all off' : 'Switch on'}</ThemedText>
              </Pressable>
            )}
          </View>
          {overview.policies.map((policy) => (
            <View key={policy.id} style={styles.policy}>
              <ThemedText type="small">{policy.id} v{policy.version} · {policy.operation}</ThemedText>
              <View style={styles.row}>
                {MODES.map(({ mode, label }) => (
                  <Pressable
                    key={mode}
                    accessibilityRole="button"
                    accessibilityState={{ selected: policy.mode === mode }}
                    disabled={policy.mode === mode}
                    onPress={() => run(() => setMutationPolicyMode(config, policy.id, mode), mode === 'enforce' ? `Enforce ${policy.id}? Changes inside it will run without asking you.` : undefined)}
                    style={[styles.button, { borderColor: theme.surfaceBorder }, policy.mode === mode && { backgroundColor: theme.backgroundElement }]}>
                    <ThemedText type="small">{label}</ThemedText>
                  </Pressable>
                ))}
              </View>
            </View>
          ))}
        </>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  container: { paddingLeft: 26, paddingRight: Spacing.three, paddingTop: Spacing.four, gap: Spacing.two },
  row: { flexDirection: 'row', alignItems: 'center', gap: Spacing.two, flexWrap: 'wrap' },
  grow: { flex: 1 },
  policy: { gap: Spacing.one },
  button: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 999, paddingHorizontal: Spacing.three, paddingVertical: 6 },
  error: { color: '#E5636D' },
});
