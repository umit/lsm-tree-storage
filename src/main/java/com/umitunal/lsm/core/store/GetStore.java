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

package com.umitunal.lsm.core.store;

import com.umitunal.lsm.memtable.MemTable;
import com.umitunal.lsm.sstable.SSTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Class that encapsulates the get operation logic for the LSM store.
 * This class follows a traditional object-oriented approach.
 */
public final class GetStore {
    private static final Logger logger = LoggerFactory.getLogger(GetStore.class);

    /**
     * Result codes for get operations
     */
    public static final int RESULT_SUCCESS = 0;
    public static final int RESULT_INVALID_INPUT = 1;
    public static final int RESULT_KEY_NOT_FOUND = 2;

    // Dependencies
    private MemTable activeMemTable;
    private List<MemTable> immutableMemTables;
    private List<SSTable> ssTables;
    private ReadWriteLock memTableLock;

    // For successful get operations
    private byte[] retrievedValue;

    /**
     * Creates a new GetStore with the specified dependencies.
     * 
     * @param activeMemTable the active MemTable
     * @param immutableMemTables the immutable MemTables
     * @param ssTables the SSTables
     * @param memTableLock the lock for synchronizing access to the MemTables
     */
    public GetStore(MemTable activeMemTable, List<MemTable> immutableMemTables, 
                   List<SSTable> ssTables, ReadWriteLock memTableLock) {
        if (activeMemTable == null) throw new IllegalArgumentException("activeMemTable cannot be null");
        if (immutableMemTables == null) throw new IllegalArgumentException("immutableMemTables cannot be null");
        if (ssTables == null) throw new IllegalArgumentException("ssTables cannot be null");
        if (memTableLock == null) throw new IllegalArgumentException("memTableLock cannot be null");

        this.activeMemTable = activeMemTable;
        this.immutableMemTables = immutableMemTables;
        this.ssTables = ssTables;
        this.memTableLock = memTableLock;
    }

    /**
     * Updates the dependencies of this GetStore.
     * This is useful when the state of the store changes.
     * 
     * @param activeMemTable the new active MemTable
     * @param immutableMemTables the new immutable MemTables
     * @param ssTables the new SSTables
     * @param memTableLock the lock for synchronizing access to the MemTables
     */
    public void updateDependencies(
            MemTable activeMemTable,
            List<MemTable> immutableMemTables,
            List<SSTable> ssTables,
            ReadWriteLock memTableLock) {
        if (activeMemTable == null) throw new IllegalArgumentException("activeMemTable cannot be null");
        if (immutableMemTables == null) throw new IllegalArgumentException("immutableMemTables cannot be null");
        if (ssTables == null) throw new IllegalArgumentException("ssTables cannot be null");
        if (memTableLock == null) throw new IllegalArgumentException("memTableLock cannot be null");

        this.activeMemTable = activeMemTable;
        this.immutableMemTables = immutableMemTables;
        this.ssTables = ssTables;
        this.memTableLock = memTableLock;
    }

    /**
     * Gets the value retrieved by the last successful get operation.
     * 
     * @return the retrieved value, or null if the last get operation was not successful
     */
    public byte[] getRetrievedValue() {
        return retrievedValue;
    }

    /**
     * Gets a value by key.
     * 
     * @param key the key as byte array
     * @return the result code of the operation
     */
    public int get(byte[] key) {
        // Reset retrieved value
        retrievedValue = null;

        // Validate input
        if (key == null || key.length == 0) {
            return RESULT_INVALID_INPUT;
        }

        // Search in memory first (active and immutable MemTables)
        byte[] result = getFromMemTables(key);

        // If not found in memory, search in SSTables
        if (result == null) {
            result = getFromSSTables(key);
        }

        if (result != null) {
            retrievedValue = result;
            return RESULT_SUCCESS;
        } else {
            return RESULT_KEY_NOT_FOUND;
        }
    }

    /**
     * Searches for a key in the active and immutable MemTables.
     * 
     * @param key the key to search for
     * @return the value if found, null otherwise
     */
    private byte[] getFromMemTables(byte[] key) {
        // First, check the active MemTable
        byte[] result = null;

        memTableLock.readLock().lock();
        try {
            result = activeMemTable.get(key);
        } finally {
            memTableLock.readLock().unlock();
        }

        // If not found, check immutable MemTables (newest to oldest)
        if (result == null) {
            synchronized (immutableMemTables) {
                for (int i = immutableMemTables.size() - 1; i >= 0; i--) {
                    MemTable memTable = immutableMemTables.get(i);
                    result = memTable.get(key);
                    if (result != null) {
                        break;
                    }
                }
            }
        }

        return result;
    }

    /**
     * Searches for a key in the SSTables.
     * Uses bloom filters for efficient negative lookups.
     * 
     * @param key the key to search for
     * @return the value if found, null otherwise
     */
    private byte[] getFromSSTables(byte[] key) {
        synchronized (ssTables) {
            // Search SSTables from newest to oldest
            for (int i = ssTables.size() - 1; i >= 0; i--) {
                SSTable ssTable = ssTables.get(i);
                // Use bloom filter for efficient negative lookups
                if (ssTable.mightContain(key)) {
                    byte[] result = ssTable.get(key);
                    if (result != null) {
                        return result;
                    }
                }
            }
        }

        return null;
    }
}
