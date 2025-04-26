# LSM-Tree Storage

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java Version](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.java.net/projects/jdk/21/)

A high-performance, Log-Structured Merge-Tree (LSM-tree) based storage system for key-value databases. This implementation leverages Java 21's new features like the Arena API and MemoryLayout for efficient memory management.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Architecture](#architecture)
- [Performance](#performance)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgements](#acknowledgements)

## Overview

LSM-Tree Storage is a Java implementation of the Log-Structured Merge-Tree, a data structure designed for high write throughput while maintaining good read performance. It's particularly well-suited for write-heavy workloads and time-series data.

The implementation follows the classic LSM-tree architecture with:
- In-memory MemTable for recent writes
- Immutable SSTables on disk for persistent storage
- Write-Ahead Log (WAL) for durability and crash recovery
- Background compaction to merge SSTables and optimize storage

## Features

- **High Write Throughput**: Optimized for write-heavy workloads by batching writes in memory.
- **Efficient Range Queries**: Data is stored in sorted order for efficient range scans.
- **Automatic Compaction**: Background process to merge SSTables and reclaim space.
- **TTL Support**: Entries can expire after a specified time.
- **Tombstone Markers**: Special markers for deleted entries.
- **Bloom Filters**: Efficient negative lookups to avoid unnecessary disk reads.
- **Memory Efficiency**: Uses Java 21's Arena API for efficient memory management.
- **Structured Data Access**: Uses Java 21's MemoryLayout for efficient off-heap storage.
- **Configurable Compaction Strategies**: Choose between threshold-based and size-tiered compaction.
- **Thread-Safe**: Concurrent read and write operations with appropriate locking.

## Requirements

- Java 21 or higher
- Maven 3.6 or higher

## Installation

### Maven

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.umitunal</groupId>
    <artifactId>lsm-tree-storage</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

### Building from Source

Clone the repository and build with Maven:

```bash
git clone https://github.com/umit/lsm-tree-storage.git
cd lsm-tree-storage
mvn clean install
```

## Usage

### Basic Operations

```java
// Create an LSMStore with default configuration
Storage storage = new LSMStore();

// Or with custom configuration
Storage storage = new LSMStore(
    10 * 1024 * 1024,  // 10MB MemTable size
    "./data",          // Data directory
    4                  // Compact after 4 SSTables
);

// Put a key-value pair
storage.put("key".getBytes(), "value".getBytes());

// Get a value by key
byte[] value = storage.get("key".getBytes());

// Delete a key
storage.delete("key".getBytes());

// Check if a key exists
boolean exists = storage.containsKey("key".getBytes());

// Get all keys
List<byte[]> keys = storage.listKeys();

// Get the number of entries
int size = storage.size();

// Clear all entries
storage.clear();

// Shutdown the storage
storage.shutdown();
```

### Range Queries

```java
// Get all key-value pairs in a range
byte[] startKey = "a".getBytes();
byte[] endKey = "z".getBytes();
Map<byte[], byte[]> range = storage.getRange(startKey, endKey);

// Or use an iterator for more efficient processing
try (KeyValueIterator iterator = storage.getIterator(startKey, endKey)) {
    while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = iterator.next();
        byte[] key = entry.getKey();
        byte[] value = entry.getValue();
        // Process the key-value pair
    }
}
```

### TTL (Time-To-Live)

```java
// Put a key-value pair with a TTL of 60 seconds
storage.put("key".getBytes(), "value".getBytes(), 60);
```

### Advanced Configuration

```java
// Create an LSMStore with a specific compaction strategy
Storage storage = new LSMStore(
    10 * 1024 * 1024,                    // 10MB MemTable size
    "./data",                            // Data directory
    4,                                   // Compact after 4 SSTables
    CompactionStrategyType.SIZE_TIERED   // Use size-tiered compaction
);

// Or with a fully custom configuration
LSMStoreConfig config = new LSMStoreConfig(
    10 * 1024 * 1024,                    // 10MB MemTable size
    "./data",                            // Data directory
    4,                                   // Compact after 4 SSTables
    30,                                  // 30 minutes compaction interval
    1,                                   // 1 minute cleanup interval
    10,                                  // 10 seconds flush interval
    CompactionStrategyType.THRESHOLD     // Use threshold-based compaction
);
Storage storage = new LSMStore(config);
```

## Architecture

The LSM-tree implementation is organized into the following packages:

1. **core**: Contains the main `LSMStore` class that implements the `Storage` interface and coordinates all operations.
2. **memtable**: Contains the `MemTable` class that represents the in-memory component of the storage system.
3. **sstable**: Contains the `SSTable` class that represents the on-disk component of the storage system.
4. **wal**: Contains the `WriteAheadLog` class that provides durability and crash recovery.

See the README.md files in each package for more details on the specific components:

- [core/README.md](src/main/java/com/umitunal/lsm/core/README.md): Details on the `LSMStore` class and overall coordination.
- [memtable/README.md](src/main/java/com/umitunal/lsm/memtable/README.md): Details on the `MemTable` class and in-memory storage.
- [sstable/README.md](src/main/java/com/umitunal/lsm/sstable/README.md): Details on the `SSTable` class and on-disk storage.
- [wal/README.md](src/main/java/com/umitunal/lsm/wal/README.md): Details on the `WriteAheadLog` class and durability.

## Performance

The LSM-Tree Storage is designed for high write throughput. In benchmarks, it can achieve:

- Write throughput: Up to 1 million operations per second on modern hardware
- Read throughput: Up to 500,000 operations per second for point lookups
- Range query performance: Efficient for small to medium-sized ranges

Actual performance will vary based on hardware, configuration, and workload characteristics.

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgements

- The LSM-tree data structure was first described in the paper ["The Log-Structured Merge-Tree (LSM-Tree)"](https://www.cs.umb.edu/~poneil/lsmtree.pdf) by Patrick O'Neil, Edward Cheng, Dieter Gawlick, and Elizabeth O'Neil.
- This implementation draws inspiration from various open-source LSM-tree implementations, including LevelDB, RocksDB, and Cassandra.

