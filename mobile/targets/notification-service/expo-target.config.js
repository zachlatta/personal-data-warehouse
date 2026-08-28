// Notification Service Extension target, added at prebuild by
// @bacons/apple-targets. It exists so pushes with an image (richContent)
// show it on iOS; see NotificationService.swift.
/** @type {import('@bacons/apple-targets/app.plugin').ConfigFunction} */
module.exports = (config) => ({
  type: 'notification-service',
  name: 'PDWNotificationService',
  bundleIdentifier: `${config.ios.bundleIdentifier}.notification-service`,
  deploymentTarget: '15.1',
  entitlements: {},
});
