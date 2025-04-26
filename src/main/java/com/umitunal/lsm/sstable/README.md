# SSTable Package

This package contains the refactored implementation of the Sorted String Table (SSTable) for the LSM-tree storage system. The implementation has been split into smaller, more focused components to improve maintainability, testability, and extensibility.

## Package Structure

The SSTable implementation has been refactored into the following packages:

1. **sstable**: Contains the main interfaces and classes for the SSTable implementation.
   - `SSTableInterface`: Interface for SSTable operations.
   - `SSTableImpl`: Implementation of the SSTableInterface that delegates to the appropriate components.
   - `SSTableAdapter`: Adapter class that adapts the new SSTableInterface to the old SSTable class.
   - `SSTableFactory`: Factory for creating SSTable instances with all the necessary components.
   - `SSTableEntry`: Record representing an entry in an SSTable.
   - `SSTableIterator`: Sealed interface for SSTable iterators.
   - `BloomFilter`: Implementation of a Bloom filter for efficient negative lookups.

2. **sstable.data**: Contains classes for data file management.
   - `DataFileManager`: Interface for managing SSTable data files.
   - `DataFileManagerImpl`: Implementation of the DataFileManager interface.

3. **sstable.index**: Contains classes for index file management.
   - `IndexFileManager`: Interface for managing SSTable index files.
   - `IndexFileManagerImpl`: Implementation of the IndexFileManager interface.

4. **sstable.filter**: Contains classes for Bloom filter management.
   - `FilterManager`: Interface for managing Bloom filters for SSTables.
   - `FilterManagerImpl`: Implementation of the FilterManager interface.

5. **sstable.io**: Contains classes for I/O operations.
   - `SSTableIO`: Interface for SSTable I/O operations.
   - `SSTableIOImpl`: Implementation of the SSTableIO interface.

6. **sstable.iterator**: Contains classes for iterator operations.
   - `SSTableIteratorFactory`: Factory interface for creating SSTable iterators.
   - `SSTableIteratorFactoryImpl`: Implementation of the SSTableIteratorFactory interface.

## Usage

To use the refactored SSTable implementation, you can use the `SSTableFactory` class to create SSTable instances:

```java
// Create a new SSTable from a MemTable
SSTableInterface sstable = SSTableFactory.createFromMemTable(
    memTable,
    directory,
    level,
    sequenceNumber
);

// Open an existing SSTable from disk
SSTableInterface sstable = SSTableFactory.openFromDisk(
    directory,
    level,
    sequenceNumber
);

// Create a backward-compatible SSTable instance
SSTable sstable = SSTableFactory.createBackwardCompatible(
    memTable,
    directory,
    level,
    sequenceNumber
);
```

## Backward Compatibility

To maintain backward compatibility with code that uses the old SSTable class, the `SSTableAdapter` class is provided. This class adapts the new SSTableInterface to the old SSTable class, allowing existing code to continue working without changes.

The `SSTableFactory.createBackwardCompatible` method creates an SSTable instance that implements the old SSTable interface, making it easy to use the new implementation with existing code.

## Benefits of the Refactoring

The refactoring of the SSTable implementation provides several benefits:

1. **Separation of Concerns**: Each class has a single responsibility, making the code easier to understand and maintain.
2. **Testability**: Each component can be tested independently, making it easier to write comprehensive tests.
3. **Flexibility**: The use of interfaces allows for different implementations of each component, making the system more flexible.
4. **Extensibility**: New record types or file formats can be added without modifying existing code.
5. **Readability**: The code is more organized and easier to navigate.

## Future Improvements

Future improvements to the SSTable implementation could include:

1. **Caching**: Add a cache for frequently accessed data.
2. **Compression**: Add support for compressing data on disk.
3. **Encryption**: Add support for encrypting data on disk.
4. **Metrics**: Add support for collecting metrics on SSTable operations.
5. **Snapshots**: Add support for creating snapshots of SSTables.