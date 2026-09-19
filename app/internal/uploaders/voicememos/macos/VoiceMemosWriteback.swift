// Voice Memos write-back helper.
//
// Renames auto-named recordings inside the Voice Memos CloudRecordings.db
// store through a real Core Data save, so the change records persistent
// history and voicememod exports it to CloudKit like a rename made in the
// app. Built on demand by the pdw CLI (app/internal/uploaders/voicememos,
// storewriter.go) and driven over stdin/stdout:
//
//   stdin:  {"store_path": ..., "model_path": ..., "author": ...,
//            "dry_run": bool, "items": [{"unique_id", "recording_id",
//            "old_title", "new_title"}, ...]}
//   stdout: [{"unique_id": ..., "status": renamed|would_rename|
//            skipped_not_auto_named|skipped_missing}, ...]
//
// model_path is the store's own cached managed object model (Z_MODELCACHE,
// already inflated by the caller), so the exact model the daemon last saved
// with is used, never a bundled copy. The store is opened with migration
// disabled: an incompatible model fails loudly instead of rewriting it.

import CoreData
import Foundation

struct PlanItem: Decodable {
    let unique_id: String
    let recording_id: String
    let old_title: String
    let new_title: String
}

struct Request: Decodable {
    let store_path: String
    let model_path: String
    let author: String
    let dry_run: Bool
    let items: [PlanItem]
}

struct Result: Encodable {
    let unique_id: String
    let status: String
}

let autoNamedFlag: Int64 = 0x1000
let defaultTitlePattern = try! NSRegularExpression(pattern: "^New Recording \\d+$")

func isAutoNamed(_ title: String, _ flags: Int64) -> Bool {
    if flags & autoNamedFlag != 0 {
        return true
    }
    let range = NSRange(title.startIndex..<title.endIndex, in: title)
    return defaultTitlePattern.firstMatch(in: title, options: [], range: range) != nil
}

func fail(_ message: String) -> Never {
    FileHandle.standardError.write((message + "\n").data(using: .utf8)!)
    exit(1)
}

func loadModel(_ path: String) -> NSManagedObjectModel {
    guard let data = FileManager.default.contents(atPath: path) else {
        fail("could not read cached model archive: \(path)")
    }
    let unarchiver: NSKeyedUnarchiver
    do {
        unarchiver = try NSKeyedUnarchiver(forReadingFrom: data)
    } catch {
        fail("could not decode cached model archive: \(error)")
    }
    unarchiver.requiresSecureCoding = false
    guard let model = unarchiver.decodeObject(forKey: "root") as? NSManagedObjectModel else {
        fail("cached model archive did not contain a managed object model")
    }
    return model
}

func openStore(_ path: String, _ model: NSManagedObjectModel) -> NSPersistentContainer {
    let description = NSPersistentStoreDescription(url: URL(fileURLWithPath: path))
    // Never migrate someone else's store.
    description.shouldMigrateStoreAutomatically = false
    description.shouldInferMappingModelAutomatically = false
    description.shouldAddStoreAsynchronously = false
    // Record persistent history for our save (what CloudKit mirroring
    // exports) and post the cross-process change notification the daemon
    // and app listen for.
    description.setOption(true as NSNumber, forKey: NSPersistentHistoryTrackingKey)
    description.setOption(true as NSNumber, forKey: NSPersistentStoreRemoteChangeNotificationPostOptionKey)
    let container = NSPersistentContainer(name: "VoiceMemos", managedObjectModel: model)
    container.persistentStoreDescriptions = [description]
    var loadError: Error?
    container.loadPersistentStores { _, error in loadError = error }
    if let error = loadError {
        fail("could not open Voice Memos store: \(error)")
    }
    return container
}

let input = FileHandle.standardInput.readDataToEndOfFile()
let request: Request
do {
    request = try JSONDecoder().decode(Request.self, from: input)
} catch {
    fail("invalid request: \(error)")
}

let model = loadModel(request.model_path)
let container = openStore(request.store_path, model)
let context = container.viewContext
context.transactionAuthor = request.author
context.name = "pdw-voice-memos-writeback"
context.mergePolicy = NSMergeByPropertyObjectTrumpMergePolicy

var results: [Result] = []
var changed = false
for item in request.items {
    let fetch = NSFetchRequest<NSManagedObject>(entityName: "CloudRecording")
    fetch.predicate = NSPredicate(format: "uniqueID == %@", argumentArray: [item.unique_id])
    let matches: [NSManagedObject]
    do {
        matches = try context.fetch(fetch)
    } catch {
        fail("fetch failed for \(item.unique_id): \(error)")
    }
    guard let recording = matches.first else {
        results.append(Result(unique_id: item.unique_id, status: "skipped_missing"))
        continue
    }
    let currentTitle = (recording.value(forKey: "encryptedTitle") as? String) ?? ""
    let currentFlags = (recording.value(forKey: "flags") as? NSNumber)?.int64Value ?? 0
    if !isAutoNamed(currentTitle, currentFlags) {
        results.append(Result(unique_id: item.unique_id, status: "skipped_not_auto_named"))
        continue
    }
    if request.dry_run {
        results.append(Result(unique_id: item.unique_id, status: "would_rename"))
        continue
    }
    recording.setValue(item.new_title, forKey: "encryptedTitle")
    recording.setValue(item.new_title, forKey: "customLabelForSorting")
    recording.setValue(NSNumber(value: currentFlags & ~autoNamedFlag), forKey: "flags")
    results.append(Result(unique_id: item.unique_id, status: "renamed"))
    changed = true
}

if changed {
    do {
        try context.save()
    } catch {
        fail("Voice Memos store save failed: \(error)")
    }
}

let encoder = JSONEncoder()
let output = try! encoder.encode(results)
FileHandle.standardOutput.write(output)
FileHandle.standardOutput.write("\n".data(using: .utf8)!)
