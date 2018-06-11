import Foundation
import CryptoSwift

let args = ProcessInfo().arguments
let path = args.count > 1 ? args[2] : "."

// Index files
let fm = FileManager()
let enumerator = fm.enumerator(atPath: path)!

var pathsByFileSize: [UInt64: [String]] = [:]
var results: [String: [String]] = [:]

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
    }
}

// Construct the paths by file size first
print("Making duplicate candidate list")
while let path = enumerator.nextObject() as? String {
    
    // skip directories
    var isDirectory: ObjCBool = false
    guard fm.fileExists(atPath: path, isDirectory: &isDirectory), !isDirectory.boolValue else {
        continue
    }
    
    let attributes = try fm.attributesOfItem(atPath: path)
    let size = attributes[.size] as! UInt64
    addSize(size, forFile: path)
    
}

let pathsToChecksum: [String] = pathsByFileSize.filter { _, paths in paths.count > 0 }.flatMap { _, paths -> [String] in
    if paths.count <= 1 {
        return []
    }
    
    return paths
}
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
                try fm.removeItem(atPath: path)
                clonefile(masterPath, path, 0)
            } else {
                fatalError()
            }
        } catch {
            print("error: \(error)")
        }
    }
}

print("Done.")
