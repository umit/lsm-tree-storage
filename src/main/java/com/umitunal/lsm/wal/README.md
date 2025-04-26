# WAL Package

This package contains the Write-Ahead Log (WAL) implementation for the LSM-tree storage system. The WAL provides durability by logging operations before they are applied to the MemTable. In case of a crash, the WAL can be replayed to recover the state of the MemTable.

## Package Structure

The WAL implementation has been refactored into the following packages:

1. **wal**: Contains the main `WAL` interface and its implementation `WALImpl`.
2. **wal.record**: Contains the record classes for different types of operations.
3. **wal.file**: Contains the file management classes.
4. **wal.manager**: Contains the manager classes for coordinating WAL operations.
5. **wal.reader**: Contains the reader classes for reading records from WAL files.
6. **wal.writer**: Contains the writer classes for writing records to WAL files.

## Components

### WAL Interface

The `WAL` interface defines the contract for the WAL system. It includes methods for appending records, reading records, and managing WAL files.

### Record Classes

- **Record**: Interface for WAL records.
- **PutRecord**: Record for a put operation.
- **DeleteRecord**: Record for a delete operation.

### File Management

- **WALFile**: Interface for WAL file operations.
- **WALFileImpl**: Implementation of the WALFile interface.

### Manager

- **WALManager**: Interface for managing WAL files.
- **WALManagerImpl**: Implementation of the WALManager interface.

### Reader

- **WALReader**: Interface for reading records from WAL files.
- **WALReaderImpl**: Implementation of the WALReader interface.

### Writer

- **WALWriter**: Interface for writing records to WAL files.
- **WALWriterImpl**: Implementation of the WALWriter interface.

## Usage

To use the WAL system, create an instance of `WALImpl` and use it through the `WAL` interface:

```java
// Create a WAL
WAL wal = new WALImpl("./data/wal");

// Append a put record
wal.appendPutRecord("key".getBytes(), "value".getBytes(), 0);

// Append a delete record
wal.appendDeleteRecord("key".getBytes());

// Read all records
List<Record> records = wal.readRecords();

// Close the WAL
wal.close();
```

## Benefits of the Refactoring

The refactoring of the WAL implementation provides several benefits:

1. **Separation of Concerns**: Each class has a single responsibility, making the code easier to understand and maintain.
2. **Testability**: Each component can be tested independently, making it easier to write comprehensive tests.
3. **Flexibility**: The use of interfaces allows for different implementations of each component, making the system more flexible.
4. **Extensibility**: New record types or file formats can be added without modifying existing code.
5. **Readability**: The code is more organized and easier to navigate.