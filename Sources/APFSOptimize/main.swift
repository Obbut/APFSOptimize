import Foundation
import CryptoSwift

let args = ProcessInfo().arguments
let paths = args.dropFirst()

// Index files
let fm = FileManager()

var pathsByFileSize: [UInt64: [String]] = [:]
var results: [String: [String]] = [:]
var savedSize: UInt64 = 0

// for progress reporting
var totalCount = 0
var hashedCount = 0
var lastReportedPercentage = 0

let resultsMutationQueue = DispatchQueue(label: "resultsMutationQueue")
let hashingQueue = DispatchQueue(label: "hashingQueue", attributes: .concurrent)
let group = DispatchGroup()

func addSize(_ size: UInt64, forFile file: String) {
    if var current = pathsByFileSize[size] {
        current.append(file)
        pathsByFileSize[size] = current
    } else {
        pathsByFileSize[size] = [file]
    }
}

func addHash(_ hash: String, forFile file: String) {
    resultsMutationQueue.sync {
        if var current = results[hash] {
            current.append(file)
            results[hash] = current
        } else {
            results[hash] = [file]
        }

        hashedCount += 1
        let percentage = Int((Double(hashedCount) / Double(totalCount)) * 100)

        if percentage > lastReportedPercentage {
            lastReportedPercentage = percentage
            print("\(percentage)% done")
        }
    }
}

// Construct the paths by file size first
for basePath in paths {
    print("Making duplicate candidate list for \(basePath)")
    
    let enumerator = fm.enumerator(atPath: basePath)!
    while let relativePath = enumerator.nextObject() as? String {
        let fullURL = URL(fileURLWithPath: basePath).appendingPathComponent(relativePath)
        let attributes = try fm.attributesOfItem(atPath: fullURL.path)
        
        guard attributes[.type] as? FileAttributeType == .typeRegular else {
            continue // Do not include symlinks, directories, etc
        }
        
        let size = attributes[.size] as! UInt64
        addSize(size, forFile: fullURL.path)
    }
}

let pathsToChecksum: [String] = pathsByFileSize.filter { _, paths in paths.count > 0 }.flatMap { _, paths -> [String] in
    if paths.count <= 1 {
        return []
    }

    return paths
}
totalCount = pathsToChecksum.count
pathsByFileSize.removeAll(keepingCapacity: false) // not needed anymore, save some memory

print("Generating checksums for \(pathsToChecksum.count) files")

for path in pathsToChecksum {

    // queue hash
    group.enter()
    hashingQueue.async {
        defer { group.leave() }

        guard let handle = FileHandle(forReadingAtPath: path) else {
            print("Could not read data from \(path)")
            return
        }

        do {
            var hash = SHA2(variant: .sha256)

            var data: Data!
            repeat {
                try autoreleasepool {
                    data = handle.readData(ofLength: 100000000)

                    guard data.count > 0 else {
                        return
                    }

                    _ = try hash.update(withBytes: Array(data))
                }
            } while data.count > 0

            let finishedHash = try hash.finish().toHexString()
            addHash(finishedHash, forFile: path)
        } catch {
            print("Error while hashing \(path): \(error)")
        }
    }

}

group.wait()

// Filter on hashes with multiple files
results = results.filter { hash, paths in
    return paths.count > 1
}

print("Indexing finished - \(results.count) unique hashes found")

// Clone the files
for (_, paths) in results {
    var paths = paths
    let masterPath = paths.removeFirst()

    for path in paths {
        do {
            print("deduplicating: \(path) from \(masterPath)")

            if #available(OSX 10.12, *) {
                // TODO: Validate equality
                let attributes = try fm.attributesOfItem(atPath: path)
                try fm.removeItem(atPath: path)
                clonefile(masterPath, path, 0)

                try fm.setAttributes(attributes, ofItemAtPath: path)

                if let size = attributes[.size] as? UInt64 {
                    savedSize += size
                }
            } else {
                fatalError()
            }
        } catch {
            print("error: \(error)")
        }
    }
}

print("Done. Potential savings: \(Int(Double(savedSize) / 1000_000)) MB")
