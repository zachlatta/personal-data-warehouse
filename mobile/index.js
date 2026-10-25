// The app (expo-router) and the launcher's left page, a second surface that
// shows only the Feed (src/features/feed/left-screen.tsx).
import 'expo-router/entry';
import { AppRegistry } from 'react-native';

import LeftScreen from './src/features/feed/left-screen';

AppRegistry.registerComponent('leftscreen', () => LeftScreen);
