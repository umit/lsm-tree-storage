# LSM MemTable Package

This package contains the in-memory component of the LSM-tree implementation:

## MemTable

The `MemTable` class is responsible for storing recent writes in memory before they are flushed to disk as SSTables. It provides the following features:

- Uses a sorted map structure (`ConcurrentSkipListMap`) for efficient range queries and iteration
- Leverages Java 21's Arena API and MemorySegment for efficient off-heap memory management
- Supports TTL (time-to-live) for entries
- Implements tombstone markers for deleted entries
- Provides thread-safe operations with atomic updates

The `MemTable` has two states:
1. **Active**: Can accept new writes and modifications
2. **Immutable**: Read-only, waiting to be flushed to disk as an SSTable

When a `MemTable` reaches its configured size limit, it becomes immutable and a new active `MemTable` is created. The immutable `MemTable` is then scheduled for flushing to disk as an SSTable.

The `MemTable` also includes a nested `ValueEntry` class that stores the actual value along with metadata like expiration time and tombstone markers. The `ValueEntry` class uses Java 21's MemorySegment API to store values in off-heap memory, which provides several benefits:

- Reduces garbage collection pressure by keeping large values outside the Java heap
- Provides direct access to memory without unnecessary copying
- Enables efficient memory management through the Arena API
- Ensures memory safety with bounds checking
- Automatically releases memory when the Arena is closed

This off-heap storage approach is particularly beneficial for large values and high-throughput scenarios where garbage collection pauses could impact performance.

This package is a critical component of the LSM-tree implementation, providing the fast write path that makes LSM-trees efficient for write-heavy workloads.
