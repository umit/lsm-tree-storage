# LSM Core Package

This package contains the core components of the LSM-tree implementation:

## LSMStore

The `LSMStore` class is the main implementation of the `Storage` interface using a Log-Structured Merge-Tree approach. It coordinates all the other components:

- Uses `MemTable` for in-memory storage of recent writes
- Uses `SSTable` for on-disk persistent storage
- Uses `WriteAheadLog` for durability and crash recovery
- Implements compaction to optimize storage and query performance
- Supports TTL (time-to-live) for entries
- Provides efficient range queries

The `LSMStore` is responsible for:
- Managing the lifecycle of MemTables and SSTables
- Coordinating reads, writes, and deletes across all components
- Scheduling background tasks like compaction and TTL cleanup
- Ensuring thread safety with appropriate locking mechanisms
- Recovering from crashes using the Write-Ahead Log

This package serves as the entry point for the LSM-tree implementation and provides the public API through the `Storage` interface.