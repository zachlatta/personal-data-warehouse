import { ActionSheetIOS, Modal, Platform, Pressable, StyleSheet, View } from 'react-native';
import { SafeAreaProvider } from 'react-native-safe-area-context';

import { ThemedText } from '@/components/themed-text';
import { Spacing } from '@/constants/theme';
import { useModalInsets } from '@/hooks/use-modal-insets';
import { useTheme } from '@/hooks/use-theme';

export type ActionSheetRequest = {
  title?: string;
  // The last option is Cancel; pick receives its index too.
  options: string[];
  destructiveIndex?: number;
  pick: (index: number) => void;
};

// A menu of more than three choices. iOS has a native action sheet; Android's
// Alert shows at most three buttons and silently drops the rest, so there the
// same request opens a bottom sheet. Render <ActionSheet> once and pass
// requests to showActionSheet.
export function showActionSheet(request: ActionSheetRequest, openSheet: (request: ActionSheetRequest) => void): void {
  if (Platform.OS === 'ios') {
    ActionSheetIOS.showActionSheetWithOptions(
      { options: request.options, cancelButtonIndex: request.options.length - 1, destructiveButtonIndex: request.destructiveIndex, title: request.title },
      request.pick,
    );
    return;
  }
  openSheet(request);
}

export function ActionSheet({ request, onClose }: { request: ActionSheetRequest | null; onClose: () => void }) {
  if (!request) return null;
  const choose = (index: number) => {
    onClose();
    request.pick(index);
  };
  return (
    <Modal transparent animationType="fade" statusBarTranslucent navigationBarTranslucent onRequestClose={() => choose(request.options.length - 1)}>
      <SafeAreaProvider>
        <Sheet request={request} choose={choose} />
      </SafeAreaProvider>
    </Modal>
  );
}

function Sheet({ request, choose }: { request: ActionSheetRequest; choose: (index: number) => void }) {
  const theme = useTheme();
  const insets = useModalInsets();
  const cancel = request.options.length - 1;
  return (
    <Pressable style={styles.scrim} onPress={() => choose(cancel)} accessibilityLabel="Close menu">
      <Pressable style={[styles.sheet, { backgroundColor: theme.backgroundElement, paddingBottom: insets.bottom + Spacing.two }]}>
        {request.title ? (
          <ThemedText style={[styles.title, { color: theme.textSecondary }]} numberOfLines={2}>
            {request.title}
          </ThemedText>
        ) : null}
        {request.options.slice(0, cancel).map((label, index) => (
          <Pressable
            key={label}
            onPress={() => choose(index)}
            style={({ pressed }) => [styles.option, pressed && { backgroundColor: theme.backgroundSelected }]}
            accessibilityRole="button">
            <ThemedText style={[styles.optionText, index === request.destructiveIndex && styles.destructive]}>{label}</ThemedText>
          </Pressable>
        ))}
        <View style={[styles.divider, { backgroundColor: theme.backgroundSelected }]} />
        <Pressable
          onPress={() => choose(cancel)}
          style={({ pressed }) => [styles.option, pressed && { backgroundColor: theme.backgroundSelected }]}
          accessibilityRole="button">
          <ThemedText style={[styles.optionText, { color: theme.textSecondary }]}>{request.options[cancel]}</ThemedText>
        </Pressable>
      </Pressable>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  scrim: { flex: 1, justifyContent: 'flex-end', backgroundColor: 'rgba(0,0,0,0.4)' },
  sheet: { borderTopLeftRadius: 16, borderTopRightRadius: 16, paddingTop: Spacing.two },
  title: { fontSize: 13, paddingHorizontal: Spacing.four, paddingVertical: Spacing.two },
  option: { paddingHorizontal: Spacing.four, paddingVertical: Spacing.three },
  optionText: { fontSize: 16 },
  destructive: { color: '#E5636D' },
  divider: { height: StyleSheet.hairlineWidth, marginVertical: Spacing.one },
});
