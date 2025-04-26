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

package com.umitunal.lsm.memtable;

import com.umitunal.lsm.api.ByteArrayWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory component of the LSM-tree that stores recent writes.
 * Uses a sorted map structure for efficient range queries and iteration.
 * Leverages Java 21's Arena API and MemorySegment for efficient off-heap memory management.
 * 
 * Key features:
 * - Uses ConcurrentSkipListMap for thread-safe, sorted storage of key-value pairs
 * - Stores values in off-heap memory using Java 21's MemorySegment API
 * - Manages memory efficiently with Arena to reduce GC pressure
 * - Supports TTL (time-to-live) for automatic expiration of entries
 * - Implements tombstone markers for deleted entries
 * - Tracks size to support flushing to disk when full
 * - Supports immutability for safe flushing to disk
 */
public class MemTable {
    private static final Logger logger = LoggerFactory.getLogger(MemTable.class);

    // Sorted map to store key-value pairs
    private final NavigableMap<ByteArrayWrapper, ValueEntry> entries;

    // Arena for memory management
    private final Arena arena;

    // Current size in bytes (atomic for thread safety)
    private final AtomicLong sizeBytes;

    // Maximum size before flushing to disk
    private final long maxSizeBytes;

    // Flag to indicate if this MemTable is immutable (being flushed to disk)
    private volatile boolean immutable;

    /**
     * Value entry with metadata for TTL and tombstone markers.
     * Uses Java 21's MemorySegment API for efficient off-heap storage.
     * 
     * Benefits of using MemorySegment:
     * - Reduces GC pressure by storing values outside the Java heap
     * - Provides direct access to memory without copying
     * - Allows for efficient memory management with Arena
     * - Enables safe memory access with bounds checking
     * - Automatically released when the Arena is closed
     * 
     * The implementation caches metadata (expirationTime, tombstone) for performance
     * while storing the actual value data in off-heap memory.
     */
    public static class ValueEntry {
        // Define the memory layout for the entry header
        private static final MemoryLayout HEADER_LAYOUT = MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("expirationTime"),  // 8 bytes
            ValueLayout.JAVA_BOOLEAN.withName("tombstone"),    // 1 byte
            MemoryLayout.paddingLayout(3),                     // 3 bytes padding for alignment
            ValueLayout.JAVA_INT.withName("valueLength")       // 4 bytes
        ).withByteAlignment(8); // Explicitly align the struct on an 8-byte boundary

        // Get the offsets for each field in the header
        private static final long EXPIRATION_TIME_OFFSET = HEADER_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("expirationTime"));
        private static final long TOMBSTONE_OFFSET = HEADER_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("tombstone"));
        private static final long VALUE_LENGTH_OFFSET = HEADER_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("valueLength"));
        private static final long HEADER_SIZE = HEADER_LAYOUT.byteSize();

        private final MemorySegment segment;
        private final boolean hasTombstone; // Cache for quick access

        /**
         * Creates a new value entry.
         * 
         * @param value the value as byte array
         * @param ttlSeconds time-to-live in seconds, 0 means no expiration
         * @param tombstone true if this is a tombstone marker (deletion)
         * @param arena the arena to allocate memory from
         */
        public ValueEntry(byte[] value, long ttlSeconds, boolean tombstone, Arena arena) {
            this.hasTombstone = tombstone;
            long expirationTime = ttlSeconds > 0 
                ? System.currentTimeMillis() + (ttlSeconds * 1000) 
                : 0; // 0 means no expiration

            if (tombstone) {
                // For tombstones, we only need the header
                this.segment = arena.allocate(HEADER_SIZE);

                // Set the header fields
                segment.set(ValueLayout.JAVA_LONG, EXPIRATION_TIME_OFFSET, expirationTime);
                segment.set(ValueLayout.JAVA_BOOLEAN, TOMBSTONE_OFFSET, true);
                segment.set(ValueLayout.JAVA_INT, VALUE_LENGTH_OFFSET, 0);
            } else {
                // For regular entries, we need the header plus the value
                int valueLength = value != null ? value.length : 0;
                this.segment = arena.allocate(HEADER_SIZE + valueLength);

                // Set the header fields
                segment.set(ValueLayout.JAVA_LONG, EXPIRATION_TIME_OFFSET, expirationTime);
                segment.set(ValueLayout.JAVA_BOOLEAN, TOMBSTONE_OFFSET, false);
                segment.set(ValueLayout.JAVA_INT, VALUE_LENGTH_OFFSET, valueLength);

                // Copy the value data after the header
                if (valueLength > 0) {
                    segment.asSlice(HEADER_SIZE, valueLength).asByteBuffer().put(0, value, 0, valueLength);
                }
            }
        }

        /**
         * Gets the value.
         * 
         * @return the value as byte array, or null if this is a tombstone
         */
        public byte[] getValue() {
            if (isTombstone()) {
                return null;
            }

            int valueLength = segment.get(ValueLayout.JAVA_INT, VALUE_LENGTH_OFFSET);
            if (valueLength <= 0) {
                return null;
            }

            byte[] result = new byte[valueLength];
            segment.asSlice(HEADER_SIZE, valueLength).asByteBuffer().get(0, result, 0, valueLength);
            return result;
        }

        /**
         * Gets the expiration time.
         * 
         * @return the expiration time in milliseconds, 0 means no expiration
         */
        public long getExpirationTime() {
            return segment.get(ValueLayout.JAVA_LONG, EXPIRATION_TIME_OFFSET);
        }

        /**
         * Checks if this entry is expired.
         * 
         * @return true if the entry has expired, false otherwise
         */
        public boolean isExpired() {
            long expirationTime = getExpirationTime();
            return expirationTime > 0 && System.currentTimeMillis() > expirationTime;
        }

        /**
         * Checks if this is a tombstone marker.
         * 
         * @return true if this is a tombstone marker, false otherwise
         */
        public boolean isTombstone() {
            return hasTombstone || segment.get(ValueLayout.JAVA_BOOLEAN, TOMBSTONE_OFFSET);
        }

        /**
         * Gets the size of this entry in bytes.
         * 
         * @return the size in bytes
         */
        public long getSizeBytes() {
            return segment.byteSize();
        }
    }

    /**
     * Creates a new MemTable with the default maximum size.
     */
    public MemTable() {
        this(10 * 1024 * 1024); // 10MB default size
    }

    /**
     * Creates a new MemTable with a custom maximum size.
     * 
     * @param maxSizeBytes maximum size in bytes before flushing to disk
     */
    public MemTable(long maxSizeBytes) {
        this.entries = new ConcurrentSkipListMap<>();
        this.arena = Arena.ofShared();
        this.sizeBytes = new AtomicLong(0);
        this.maxSizeBytes = maxSizeBytes;
        this.immutable = false;

        logger.info("""
            MemTable created with max size: %d bytes
            """.formatted(maxSizeBytes));
    }

    /**
     * Puts a key-value pair into the MemTable.
     * 
     * @param key the key as byte array
     * @param value the value as byte array
     * @param ttlSeconds time-to-live in seconds, 0 means no expiration
     * @return true if the operation was successful, false if the MemTable is full or immutable
     */
    public boolean put(byte[] key, byte[] value, long ttlSeconds) {
        if (immutable) {
            return false; // Cannot modify an immutable MemTable
        }

        if (key == null || key.length == 0 || value == null) {
            return false;
        }

        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        ValueEntry newEntry = new ValueEntry(value, ttlSeconds, false, arena);

        // Calculate the size of the new entry
        long entrySize = key.length + newEntry.getSizeBytes();

        // Check if adding this entry would exceed the maximum size
        if (sizeBytes.get() + entrySize > maxSizeBytes) {
            return false; // MemTable is full
        }

        // Add the entry to the map
        ValueEntry oldEntry = entries.put(keyWrapper, newEntry);

        // Update the size
        if (oldEntry != null) {
            sizeBytes.addAndGet(entrySize - oldEntry.getSizeBytes());
        } else {
            sizeBytes.addAndGet(entrySize);
        }

        return true;
    }

    /**
     * Deletes a key-value pair from the MemTable by adding a tombstone marker.
     * 
     * @param key the key as byte array
     * @return true if the operation was successful, false if the MemTable is immutable
     */
    public boolean delete(byte[] key) {
        if (immutable) {
            return false; // Cannot modify an immutable MemTable
        }

        if (key == null || key.length == 0) {
            return false;
        }

        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        ValueEntry tombstone = new ValueEntry(null, 0, true, arena);

        // Add the tombstone marker to the map
        ValueEntry oldEntry = entries.put(keyWrapper, tombstone);

        // Update the size
        if (oldEntry != null) {
            sizeBytes.addAndGet(-oldEntry.getSizeBytes());
        }

        return true;
    }

    /**
     * Gets a value by key.
     * 
     * @param key the key as byte array
     * @return the value as byte array, or null if the key doesn't exist, is expired, or is a tombstone
     */
    public byte[] get(byte[] key) {
        if (key == null || key.length == 0) {
            return null;
        }

        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        ValueEntry entry = entries.get(keyWrapper);

        if (entry == null || entry.isExpired() || entry.isTombstone()) {
            return null;
        }

        return entry.getValue();
    }

    /**
     * Checks if a key exists in the MemTable.
     * 
     * @param key the key as byte array
     * @return true if the key exists and is not expired or a tombstone, false otherwise
     */
    public boolean containsKey(byte[] key) {
        if (key == null || key.length == 0) {
            return false;
        }

        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        ValueEntry entry = entries.get(keyWrapper);

        return entry != null && !entry.isExpired() && !entry.isTombstone();
    }

    /**
     * Gets all entries in the MemTable.
     * 
     * @return a map of all entries
     */
    public Map<ByteArrayWrapper, ValueEntry> getEntries() {
        return entries;
    }

    /**
     * Gets the current size of the MemTable in bytes.
     * 
     * @return the size in bytes
     */
    public long getSizeBytes() {
        return sizeBytes.get();
    }

    /**
     * Checks if the MemTable is full.
     * 
     * @return true if the MemTable is full, false otherwise
     */
    public boolean isFull() {
        return sizeBytes.get() >= maxSizeBytes;
    }

    /**
     * Makes the MemTable immutable.
     * After calling this method, no more modifications are allowed.
     */
    public void makeImmutable() {
        immutable = true;
        logger.info("MemTable made immutable with size: " + sizeBytes.get() + " bytes");
    }

    /**
     * Checks if the MemTable is immutable.
     * 
     * @return true if the MemTable is immutable, false otherwise
     */
    public boolean isImmutable() {
        return immutable;
    }

    /**
     * Closes the MemTable and releases resources.
     */
    public void close() {
        arena.close();
        logger.info("MemTable closed");
    }
}
