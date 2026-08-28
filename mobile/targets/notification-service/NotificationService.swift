import UserNotifications

// Notification Service Extension: iOS hands every push with mutable-content
// here before showing it. The Expo push service forwards a message's
// richContent under userInfo["body"]["_richContent"]; this downloads the
// image and attaches it, so the alert shows a thumbnail and a full-size
// image when expanded. Anything that fails falls through to the plain alert.
class NotificationService: UNNotificationServiceExtension {
  var contentHandler: ((UNNotificationContent) -> Void)?
  var bestAttemptContent: UNMutableNotificationContent?

  override func didReceive(
    _ request: UNNotificationRequest,
    withContentHandler contentHandler: @escaping (UNNotificationContent) -> Void
  ) {
    self.contentHandler = contentHandler
    bestAttemptContent = request.content.mutableCopy() as? UNMutableNotificationContent
    guard let content = bestAttemptContent else {
      contentHandler(request.content)
      return
    }
    guard let url = imageURL(in: request.content.userInfo) else {
      contentHandler(content)
      return
    }
    downloadAndAttach(url: url, to: content, completion: contentHandler)
  }

  private func imageURL(in userInfo: [AnyHashable: Any]) -> URL? {
    guard let body = userInfo["body"] as? [String: Any],
      let rich = body["_richContent"] as? [String: Any],
      let image = rich["image"] as? String,
      let url = URL(string: image)
    else { return nil }
    return url
  }

  private func downloadAndAttach(
    url: URL,
    to content: UNMutableNotificationContent,
    completion: @escaping (UNNotificationContent) -> Void
  ) {
    let task = URLSession.shared.downloadTask(with: url) { location, response, _ in
      guard let location = location else {
        completion(content)
        return
      }
      // UNNotificationAttachment infers the type from the file extension, so
      // give the temp file one that matches what was actually served.
      let ext = Self.fileExtension(for: response, url: url)
      let target = URL(fileURLWithPath: NSTemporaryDirectory())
        .appendingPathComponent(UUID().uuidString)
        .appendingPathExtension(ext)
      do {
        try FileManager.default.moveItem(at: location, to: target)
        content.attachments = [try UNNotificationAttachment(identifier: "image", url: target, options: nil)]
      } catch {
        // Leave the alert as-is; a missing image is better than no alert.
      }
      completion(content)
    }
    task.resume()
  }

  private static func fileExtension(for response: URLResponse?, url: URL) -> String {
    switch response?.mimeType {
    case "image/png": return "png"
    case "image/gif": return "gif"
    case "image/jpeg": return "jpg"
    case "image/heic": return "heic"
    case "image/webp": return "webp"
    default:
      let fromPath = url.pathExtension.lowercased()
      return fromPath.isEmpty ? "jpg" : fromPath
    }
  }

  override func serviceExtensionTimeWillExpire() {
    // The system is about to kill us; ship what we have rather than nothing.
    if let contentHandler = contentHandler, let content = bestAttemptContent {
      contentHandler(content)
    }
  }
}
