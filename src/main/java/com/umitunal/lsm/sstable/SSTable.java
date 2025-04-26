/*
 * Copyright (c) 2023-2025 Umit Unal
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.umitunal.lsm.sstable;

import com.umitunal.lsm.api.ByteArrayWrapper;
import com.umitunal.lsm.memtable.MemTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * Sorted String Table (SSTable) implementation for the LSM-tree.
 * This class represents the on-disk component of the LSM-tree storage system.
 * It uses memory-mapped files for efficient I/O and Java 21's MemoryLayout for structured data access.
 */
public class SSTable {
    private static final Logger logger = LoggerFactory.getLogger(SSTable.class);

    // File paths
    private final Path dataFilePath;
    private final Path indexFilePath;
    private final Path filterFilePath;

    // File channels for data access
    private FileChannel dataChannel;
    private FileChannel indexChannel;

    // Sparse index for efficient lookups
    private final NavigableMap<ByteArrayWrapper, Long> sparseIndex;

    // Bloom filter for efficient negative lookups
    private BloomFilter bloomFilter;

    // Metadata
    private final long creationTime;
    private final int level;
    private final long sequenceNumber;

    // Memory management
    private final Arena arena;

    /**
     * Creates a new SSTable from a MemTable.
     * 
     * @param memTable the MemTable to flush to disk
     * @param directory the directory to store the SSTable files
     * @param level the level of this SSTable in the LSM-tree
     * @param sequenceNumber the sequence number of this SSTable
     * @throws IOException if an I/O error occurs
     */
    public SSTable(MemTable memTable, String directory, int level, long sequenceNumber) throws IOException {
        this.creationTime = System.currentTimeMillis();
        this.level = level;
        this.sequenceNumber = sequenceNumber;
        this.sparseIndex = new TreeMap<>();
        this.arena = Arena.ofShared();

        // Create directory if it doesn't exist
        File dir = new File(directory);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        // Define file paths
        String filePrefix = String.format("sst_L%d_S%d", level, sequenceNumber);
        this.dataFilePath = Path.of(directory, filePrefix + ".data");
        this.indexFilePath = Path.of(directory, filePrefix + ".index");
        this.filterFilePath = Path.of(directory, filePrefix + ".filter");

        // Flush MemTable to disk
        flushToDisk(memTable);

        logger.info("Created SSTable: " + filePrefix);
    }

    /**
     * Opens an existing SSTable from disk.
     * 
     * @param directory the directory containing the SSTable files
     * @param level the level of this SSTable in the LSM-tree
     * @param sequenceNumber the sequence number of this SSTable
     * @throws IOException if an I/O error occurs
     */
    public SSTable(String directory, int level, long sequenceNumber) throws IOException {
        this.level = level;
        this.sequenceNumber = sequenceNumber;
        this.sparseIndex = new TreeMap<>();
        this.arena = Arena.ofShared();

        // Define file paths
        String filePrefix = String.format("sst_L%d_S%d", level, sequenceNumber);
        this.dataFilePath = Path.of(directory, filePrefix + ".data");
        this.indexFilePath = Path.of(directory, filePrefix + ".index");
        this.filterFilePath = Path.of(directory, filePrefix + ".filter");

        // Load metadata
        this.creationTime = new File(dataFilePath.toString()).lastModified();

        // Load from disk
        loadFromDisk();

        logger.info("Opened SSTable: " + filePrefix);
    }

    /**
     * Flushes a MemTable to disk as an SSTable.
     * 
     * @param memTable the MemTable to flush
     * @throws IOException if an I/O error occurs
     */
    private void flushToDisk(MemTable memTable) throws IOException {
        logger.info("Flushing MemTable to disk as SSTable");

        // Create a bloom filter for efficient negative lookups
        BloomFilter filter = new BloomFilter(memTable.getEntries().size(), 0.01);

        // Create data and index files
        try (FileChannel writeDataChannel = FileChannel.open(
                dataFilePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
             FileChannel writeIndexChannel = FileChannel.open(
                indexFilePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            // Write header to data file
            ByteBuffer headerBuffer = ByteBuffer.allocate(16);
            headerBuffer.putLong(creationTime);
            headerBuffer.putInt(level);
            headerBuffer.putInt(memTable.getEntries().size());
            headerBuffer.flip();
            writeDataChannel.write(headerBuffer);

            // Write entries to data file and build index
            long currentOffset = 16; // Start after header

            for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : memTable.getEntries().entrySet()) {
                byte[] key = entry.getKey().getData();
                MemTable.ValueEntry valueEntry = entry.getValue();

                // Skip expired entries
                if (valueEntry.isExpired()) {
                    continue;
                }

                // Add key to bloom filter
                filter.add(key);

                // Create SSTableEntry
                SSTableEntry sstEntry;
                if (valueEntry.isTombstone()) {
                    sstEntry = SSTableEntry.tombstone(key, valueEntry.getExpirationTime());
                } else {
                    sstEntry = SSTableEntry.of(key, valueEntry.getValue(), valueEntry.getExpirationTime());
                }

                // Write entry to data file
                ByteBuffer keyBuffer = ByteBuffer.allocate(4 + key.length);
                keyBuffer.putInt(key.length);
                keyBuffer.put(key);
                keyBuffer.flip();
                writeDataChannel.write(keyBuffer);

                // Write value or tombstone marker
                if (valueEntry.isTombstone()) {
                    ByteBuffer tombstoneBuffer = ByteBuffer.allocate(12); // 4 bytes for int + 8 bytes for long
                    tombstoneBuffer.putInt(0); // 0 length indicates tombstone
                    tombstoneBuffer.putLong(valueEntry.getExpirationTime());
                    tombstoneBuffer.flip();
                    writeDataChannel.write(tombstoneBuffer);
                } else {
                    byte[] value = valueEntry.getValue();
                    ByteBuffer valueBuffer = ByteBuffer.allocate(4 + value.length + 8);
                    valueBuffer.putInt(value.length);
                    valueBuffer.put(value);
                    valueBuffer.putLong(valueEntry.getExpirationTime());
                    valueBuffer.flip();
                    writeDataChannel.write(valueBuffer);
                }

                // Add to sparse index
                sparseIndex.put(entry.getKey(), currentOffset);

                // Update offset for next entry
                currentOffset = writeDataChannel.position();
            }

            // Write index to index file
            for (Map.Entry<ByteArrayWrapper, Long> indexEntry : sparseIndex.entrySet()) {
                byte[] key = indexEntry.getKey().getData();
                long offset = indexEntry.getValue();

                ByteBuffer indexBuffer = ByteBuffer.allocate(4 + key.length + 8);
                indexBuffer.putInt(key.length);
                indexBuffer.put(key);
                indexBuffer.putLong(offset);
                indexBuffer.flip();

                writeIndexChannel.write(indexBuffer);
            }
        }

        // Save bloom filter to file
        filter.save(filterFilePath.toString());

        // Load the bloom filter into memory
        this.bloomFilter = filter;

        // Open the data file for reading
        this.dataChannel = FileChannel.open(dataFilePath, StandardOpenOption.READ);

        // Open the index file for reading
        this.indexChannel = FileChannel.open(indexFilePath, StandardOpenOption.READ);

        logger.info("MemTable flushed to SSTable successfully");
    }

    /**
     * Loads an SSTable from disk.
     * 
     * @throws IOException if an I/O error occurs
     */
    private void loadFromDisk() throws IOException {
        logger.info("Loading SSTable from disk");

        // Load the bloom filter
        if (Files.exists(filterFilePath)) {
            this.bloomFilter = BloomFilter.load(filterFilePath.toString());
        } else {
            logger.warn("Bloom filter file not found: " + filterFilePath);
            this.bloomFilter = new BloomFilter(1000, 0.01); // Default filter
        }

        // Load the sparse index
        if (Files.exists(indexFilePath)) {
            try (FileChannel indexChannel = FileChannel.open(indexFilePath, StandardOpenOption.READ)) {
                ByteBuffer buffer = ByteBuffer.allocate(1024); // Initial buffer size

                while (indexChannel.position() < indexChannel.size()) {
                    // Read key length
                    buffer.clear();
                    buffer.limit(4);
                    indexChannel.read(buffer);
                    buffer.flip();
                    int keyLength = buffer.getInt();

                    // Read key
                    buffer.clear();
                    buffer.limit(keyLength);
                    if (buffer.capacity() < keyLength) {
                        buffer = ByteBuffer.allocate(keyLength);
                    }
                    indexChannel.read(buffer);
                    buffer.flip();
                    byte[] key = new byte[keyLength];
                    buffer.get(key);

                    // Read offset
                    buffer.clear();
                    buffer.limit(8);
                    indexChannel.read(buffer);
                    buffer.flip();
                    long offset = buffer.getLong();

                    // Add to sparse index
                    sparseIndex.put(new ByteArrayWrapper(key), offset);
                }
            }
        } else {
            logger.warn("Index file not found: " + indexFilePath);
        }

        // Open the data file for reading
        if (Files.exists(dataFilePath)) {
            try {
                // Open the data file channel
                dataChannel = FileChannel.open(dataFilePath, StandardOpenOption.READ);

                // Read header
                ByteBuffer headerBuffer = ByteBuffer.allocate(16);
                dataChannel.read(headerBuffer, 0);
                headerBuffer.flip();

                long storedCreationTime = headerBuffer.getLong();
                int storedLevel = headerBuffer.getInt();
                int entryCount = headerBuffer.getInt();

                logger.info("Loaded SSTable with " + entryCount + " entries, level " + 
                           storedLevel + ", creation time " + storedCreationTime);
            } catch (IOException e) {
                logger.error("Error opening data file", e);
                throw e;
            }
        } else {
            logger.warn("Data file not found: " + dataFilePath);
        }

        logger.info("SSTable loaded successfully");
    }

    /**
     * Gets a value by key.
     * 
     * @param key the key as byte array
     * @return the value as byte array, or null if the key doesn't exist
     */
    public byte[] get(byte[] key) {
        if (key == null || key.length == 0 || dataChannel == null) {
            return null;
        }

        // First check the bloom filter for a quick negative
        if (bloomFilter != null && !bloomFilter.mightContain(key)) {
            return null; // Definitely not in the set
        }

        try {
            // Find the closest key in the sparse index
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
            Map.Entry<ByteArrayWrapper, Long> indexEntry = sparseIndex.floorEntry(keyWrapper);

            if (indexEntry == null) {
                // No entry in the sparse index that is less than or equal to the key
                // Start from the beginning of the data file (after the header)
                return findKeyInDataFile(key, 16);
            } else {
                // Start searching from the position in the sparse index
                return findKeyInDataFile(key, indexEntry.getValue());
            }
        } catch (IOException e) {
            logger.error("Error reading from SSTable", e);
            return null;
        }
    }

    /**
     * Finds a key in the data file starting from the given position.
     * 
     * @param key the key to find
     * @param startPosition the position to start searching from
     * @return the value as byte array, or null if the key doesn't exist
     * @throws IOException if an I/O error occurs
     */
    private byte[] findKeyInDataFile(byte[] key, long startPosition) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024); // Initial buffer size
        long position = startPosition;

        while (position < dataChannel.size()) {
            // Read key length
            buffer.clear();
            buffer.limit(4);
            dataChannel.read(buffer, position);
            buffer.flip();
            int keyLength = buffer.getInt();
            position += 4;

            // Read key
            buffer.clear();
            buffer.limit(keyLength);
            if (buffer.capacity() < keyLength) {
                buffer = ByteBuffer.allocate(keyLength);
            }
            dataChannel.read(buffer, position);
            buffer.flip();
            byte[] entryKey = new byte[keyLength];
            buffer.get(entryKey);
            position += keyLength;

            // Read value length
            buffer.clear();
            buffer.limit(4);
            dataChannel.read(buffer, position);
            buffer.flip();
            int valueLength = buffer.getInt();
            position += 4;

            // Check if this is a tombstone
            if (valueLength == 0) {
                // Skip the timestamp (8 bytes)
                position += 8;

                // If this is the key we're looking for, it's been deleted
                if (Arrays.equals(key, entryKey)) {
                    return null;
                }

                continue;
            }

            // Read value
            buffer.clear();
            buffer.limit(valueLength);
            if (buffer.capacity() < valueLength) {
                buffer = ByteBuffer.allocate(valueLength);
            }
            dataChannel.read(buffer, position);
            buffer.flip();
            byte[] value = new byte[valueLength];
            buffer.get(value);
            position += valueLength;

            // Skip the timestamp (8 bytes)
            position += 8;

            // If this is the key we're looking for, return the value
            if (Arrays.equals(key, entryKey)) {
                return value;
            }
        }

        // Key not found
        return null;
    }

    /**
     * Checks if a key might be in this SSTable.
     * This is a fast check using the Bloom filter, which may return false positives
     * but never false negatives.
     * 
     * @param key the key as byte array
     * @return true if the key might be in this SSTable, false if it definitely isn't
     */
    public boolean mightContain(byte[] key) {
        return bloomFilter != null && bloomFilter.mightContain(key);
    }

    /**
     * Gets the level of this SSTable in the LSM-tree.
     * 
     * @return the level
     */
    public int getLevel() {
        return level;
    }

    /**
     * Gets the sequence number of this SSTable.
     * 
     * @return the sequence number
     */
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * Gets the creation time of this SSTable.
     * 
     * @return the creation time in milliseconds since the epoch
     */
    public long getCreationTime() {
        return creationTime;
    }

    /**
     * Gets the size of this SSTable in bytes.
     * 
     * @return the size in bytes
     */
    public long getSizeBytes() {
        long size = 0;

        try {
            File dataFile = dataFilePath.toFile();
            File indexFile = indexFilePath.toFile();
            File filterFile = filterFilePath.toFile();

            if (dataFile.exists()) size += dataFile.length();
            if (indexFile.exists()) size += indexFile.length();
            if (filterFile.exists()) size += filterFile.length();
        } catch (Exception e) {
            logger.warn("Error getting SSTable size", e);
        }

        return size;
    }

    /**
     * Closes this SSTable and releases resources.
     */
    public void close() {
        try {
            if (dataChannel != null && dataChannel.isOpen()) {
                dataChannel.close();
            }
            if (indexChannel != null && indexChannel.isOpen()) {
                indexChannel.close();
            }

            // Only close the arena if it's not already closed
            try {
                if (arena != null) {
                    arena.close();
                }
            } catch (IllegalStateException e) {
                // Arena is already closed, ignore
            }

            logger.info("SSTable closed: " + dataFilePath);
        } catch (IOException e) {
            logger.warn("Error closing SSTable resources", e);
        }
    }

    /**
     * Deletes all files associated with this SSTable.
     * 
     * @return true if all files were deleted successfully, false otherwise
     */
    public boolean delete() {
        boolean success = true;

        try {
            // Close first to release resources
            close();

            // Delete files
            File dataFile = dataFilePath.toFile();
            File indexFile = indexFilePath.toFile();
            File filterFile = filterFilePath.toFile();

            if (dataFile.exists() && !dataFile.delete()) {
                success = false;
            }
            if (indexFile.exists() && !indexFile.delete()) {
                success = false;
            }
            if (filterFile.exists() && !filterFile.delete()) {
                success = false;
            }
        } catch (Exception e) {
            logger.warn("Error deleting SSTable files", e);
            success = false;
        }

        return success;
    }

    /**
     * Lists all keys in this SSTable.
     * 
     * @return a list of all keys in this SSTable, excluding tombstones
     */
    public List<byte[]> listKeys() {
        List<byte[]> keys = new ArrayList<>();

        try {
            // Scan the data file to get all keys
            if (dataChannel != null && dataChannel.isOpen()) {
                // Start after the header
                long position = 16;
                ByteBuffer buffer = ByteBuffer.allocate(1024); // Initial buffer size

                while (position < dataChannel.size()) {
                    // Read key length
                    buffer.clear();
                    buffer.limit(4);
                    dataChannel.read(buffer, position);
                    buffer.flip();
                    int keyLength = buffer.getInt();
                    position += 4;

                    // Read key
                    buffer.clear();
                    buffer.limit(keyLength);
                    if (buffer.capacity() < keyLength) {
                        buffer = ByteBuffer.allocate(keyLength);
                    }
                    dataChannel.read(buffer, position);
                    buffer.flip();
                    byte[] key = new byte[keyLength];
                    buffer.get(key);
                    position += keyLength;

                    // Read value length
                    buffer.clear();
                    buffer.limit(4);
                    dataChannel.read(buffer, position);
                    buffer.flip();
                    int valueLength = buffer.getInt();
                    position += 4;

                    // Skip tombstones
                    if (valueLength == 0) {
                        // Skip the timestamp (8 bytes)
                        position += 8;
                        continue;
                    }

                    // Add the key to the list
                    keys.add(key);

                    // Skip the value and timestamp
                    position += valueLength + 8;
                }
            }

            return keys;
        } catch (Exception e) {
            logger.error("Error listing keys from SSTable", e);
            return keys;
        }
    }

    /**
     * Counts the number of entries in this SSTable.
     * 
     * @return the number of entries in this SSTable
     */
    public int countEntries() {
        try {
            // This is a simplified implementation that assumes the sparse index contains all keys
            // A more complete implementation would scan the data file and count all entries
            return sparseIndex.size();
        } catch (Exception e) {
            logger.error("Error counting entries in SSTable", e);
            return 0;
        }
    }

    /**
     * Gets all key-value pairs with keys in the specified range.
     * The range is inclusive of startKey and exclusive of endKey.
     * 
     * @param startKey the start of the range (inclusive), or null for the first key
     * @param endKey the end of the range (exclusive), or null for no upper bound
     * @return a map of keys to values in the specified range
     */
    public Map<byte[], byte[]> getRange(byte[] startKey, byte[] endKey) {
        Map<byte[], byte[]> result = new TreeMap<>((a, b) -> {
            ByteArrayWrapper wrapperA = new ByteArrayWrapper(a);
            ByteArrayWrapper wrapperB = new ByteArrayWrapper(b);
            return wrapperA.compareTo(wrapperB);
        });

        try {
            ByteArrayWrapper startWrapper = startKey != null ? new ByteArrayWrapper(startKey) : null;
            ByteArrayWrapper endWrapper = endKey != null ? new ByteArrayWrapper(endKey) : null;

            // Scan the data file to get all keys in the range
            if (dataChannel != null && dataChannel.isOpen()) {
                // Start after the header
                long position = 16;
                ByteBuffer buffer = ByteBuffer.allocate(1024); // Initial buffer size

                while (position < dataChannel.size()) {
                    // Read key length
                    buffer.clear();
                    buffer.limit(4);
                    dataChannel.read(buffer, position);
                    buffer.flip();
                    int keyLength = buffer.getInt();
                    position += 4;

                    // Read key
                    buffer.clear();
                    buffer.limit(keyLength);
                    if (buffer.capacity() < keyLength) {
                        buffer = ByteBuffer.allocate(keyLength);
                    }
                    dataChannel.read(buffer, position);
                    buffer.flip();
                    byte[] key = new byte[keyLength];
                    buffer.get(key);
                    position += keyLength;

                    // Check if the key is in the range
                    ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
                    boolean inRange = true;
                    if (startWrapper != null && keyWrapper.compareTo(startWrapper) < 0) {
                        inRange = false; // Key is before the start of the range
                    }
                    if (endWrapper != null && keyWrapper.compareTo(endWrapper) >= 0) {
                        inRange = false; // Key is at or after the end of the range
                    }

                    // Read value length
                    buffer.clear();
                    buffer.limit(4);
                    dataChannel.read(buffer, position);
                    buffer.flip();
                    int valueLength = buffer.getInt();
                    position += 4;

                    // Skip tombstones
                    if (valueLength == 0) {
                        // Skip the timestamp (8 bytes)
                        position += 8;
                        continue;
                    }

                    // If the key is in the range, add it to the result
                    if (inRange) {
                        // Read value
                        buffer.clear();
                        buffer.limit(valueLength);
                        if (buffer.capacity() < valueLength) {
                            buffer = ByteBuffer.allocate(valueLength);
                        }
                        dataChannel.read(buffer, position);
                        buffer.flip();
                        byte[] value = new byte[valueLength];
                        buffer.get(value);

                        result.put(key, value);
                    }

                    // Skip the value (if we didn't read it) and timestamp
                    position += valueLength + 8;
                }
            }

            return result;
        } catch (Exception e) {
            logger.error("Error getting range from SSTable", e);
            return result;
        }
    }

}
