import Foundation
import Photos

private let authorizedStatus = PHAuthorizationStatus.authorized.rawValue

private final class Completion: @unchecked Sendable {
    private let lock = NSLock()
    private var finished = false
    private var storedError: Error?
    private var storedStatus: PHAuthorizationStatus?

    func finish(error: Error? = nil, status: PHAuthorizationStatus? = nil) {
        lock.lock()
        storedError = error
        storedStatus = status
        finished = true
        lock.unlock()
    }

    func snapshot() -> (Bool, Error?, PHAuthorizationStatus?) {
        lock.lock()
        defer { lock.unlock() }
        return (finished, storedError, storedStatus)
    }
}

private func waitForCompletion(_ completion: Completion) -> (Error?, PHAuthorizationStatus?) {
    while true {
        let (finished, error, status) = completion.snapshot()
        if finished {
            return (error, status)
        }
        RunLoop.current.run(mode: .default, before: Date(timeIntervalSinceNow: 0.05))
    }
}

private func emit(_ payload: [String: Any]) -> Never {
    let data = try! JSONSerialization.data(withJSONObject: payload, options: [])
    FileHandle.standardOutput.write(data)
    FileHandle.standardOutput.write(Data("\n".utf8))
    exit(0)
}

private func fail(_ message: String) -> Never {
    FileHandle.standardError.write(Data((message + "\n").utf8))
    exit(1)
}

private func arguments() -> [String: String] {
    var parsed: [String: String] = [:]
    var index = 2
    while index + 1 < CommandLine.arguments.count {
        parsed[CommandLine.arguments[index]] = CommandLine.arguments[index + 1]
        index += 2
    }
    return parsed
}

private func fetchAsset(uuid: String) -> PHAsset? {
    let normalIdentifier = "\(uuid)/L0/001"
    let direct = PHAsset.fetchAssets(withLocalIdentifiers: [normalIdentifier], options: nil)
    if direct.count == 1 {
        return direct.object(at: 0)
    }
    let allAssets = PHAsset.fetchAssets(with: nil)
    var found: PHAsset?
    allAssets.enumerateObjects { asset, _, stop in
        if asset.localIdentifier.split(separator: "/", maxSplits: 1).first.map(String.init) == uuid {
            found = asset
            stop.pointee = true
        }
    }
    return found
}

private func authorize() -> Never {
    let current = PHPhotoLibrary.authorizationStatus(for: .readWrite)
    if current != .notDetermined {
        emit(["status": current.rawValue])
    }
    let completion = Completion()
    PHPhotoLibrary.requestAuthorization(for: .readWrite) { status in
        completion.finish(status: status)
    }
    let (_, status) = waitForCompletion(completion)
    emit(["status": status?.rawValue ?? -1])
}

private func exportResource() -> Never {
    guard PHPhotoLibrary.authorizationStatus(for: .readWrite).rawValue == authorizedStatus else {
        fail("The uploader does not have Full Photos library access. Run `uv run python -m personal_data_warehouse_photos.cli --authorize` interactively, then allow Full Access in System Settings → Privacy & Security → Photos.")
    }
    let values = arguments()
    guard
        let uuid = values["--uuid"],
        let role = values["--role"],
        let kind = values["--kind"],
        let requestedFilename = values["--filename"],
        let destination = values["--destination"]
    else {
        fail("Missing required export arguments")
    }
    guard let asset = fetchAsset(uuid: uuid) else {
        fail("Apple Photos asset \(uuid) was not available through PhotoKit. Confirm this is the System Photo Library.")
    }

    let wantedType: PHAssetResourceType
    if role == "live_video" {
        wantedType = .pairedVideo
    } else if kind == "video" {
        wantedType = .video
    } else {
        wantedType = .photo
    }
    let matching = PHAssetResource.assetResources(for: asset).filter { $0.type == wantedType }
    let resource = matching.first {
        $0.originalFilename.caseInsensitiveCompare(requestedFilename) == .orderedSame
    } ?? matching.first
    guard let resource else {
        fail("Apple Photos asset \(uuid) has no original \(role) resource")
    }

    let destinationURL = URL(fileURLWithPath: destination)
    try? FileManager.default.removeItem(at: destinationURL)
    let options = PHAssetResourceRequestOptions()
    options.isNetworkAccessAllowed = true
    let completion = Completion()
    PHAssetResourceManager.default().writeData(for: resource, toFile: destinationURL, options: options) {
        completion.finish(error: $0)
    }
    let (error, _) = waitForCompletion(completion)
    if let error {
        try? FileManager.default.removeItem(at: destinationURL)
        fail("Could not download the full original for \(requestedFilename) from Apple Photos/iCloud: \(error.localizedDescription)")
    }
    guard
        let attributes = try? FileManager.default.attributesOfItem(atPath: destination),
        let size = attributes[.size] as? NSNumber,
        size.int64Value > 0
    else {
        try? FileManager.default.removeItem(at: destinationURL)
        fail("PhotoKit exported an empty original for \(requestedFilename)")
    }
    emit([
        "filename": resource.originalFilename,
        "uti": resource.uniformTypeIdentifier,
        "size_bytes": size.int64Value,
    ])
}

guard CommandLine.arguments.count >= 2 else {
    fail("Expected a command: status, authorize, or export")
}

switch CommandLine.arguments[1] {
case "status":
    emit([
        "status": PHPhotoLibrary.authorizationStatus(for: .readWrite).rawValue,
        "bundle_identifier": Bundle.main.bundleIdentifier ?? "",
        "usage_description": Bundle.main.object(
            forInfoDictionaryKey: "NSPhotoLibraryUsageDescription"
        ) as? String ?? "",
    ])
case "authorize":
    authorize()
case "export":
    exportResource()
default:
    fail("Unknown command: \(CommandLine.arguments[1])")
}
