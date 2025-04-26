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
import com.umitunal.lsm.wal.WAL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Class that encapsulates the put operation logic for the LSM store.
 * This class follows a traditional object-oriented approach.
 */
public final class PutStore {
    private static final Logger logger = LoggerFactory.getLogger(PutStore.class);

    /**
     * Result codes for put operations
     */
    public static final int RESULT_SUCCESS = 0;
    public static final int RESULT_INVALID_INPUT = 1;
    public static final int RESULT_WAL_ERROR = 2;
    public static final int RESULT_MEMTABLE_FULL = 3;

    // Dependencies
    private MemTable activeMemTable;
    private ReadWriteLock memTableLock;
    private WAL wal;
    private AtomicBoolean recovering;

    // For WAL errors
    private IOException lastException;

    /**
     * Creates a new PutStore with the specified dependencies.
     * 
     * @param activeMemTable the active MemTable
     * @param memTableLock the lock for synchronizing access to the MemTables
     * @param wal the Write-Ahead Log
     * @param recovering flag indicating if the store is in recovery mode
     */
    public PutStore(MemTable activeMemTable, ReadWriteLock memTableLock, WAL wal, AtomicBoolean recovering) {
        if (activeMemTable == null) throw new IllegalArgumentException("activeMemTable cannot be null");
        if (memTableLock == null) throw new IllegalArgumentException("memTableLock cannot be null");
        if (wal == null) throw new IllegalArgumentException("wal cannot be null");
        if (recovering == null) throw new IllegalArgumentException("recovering cannot be null");

        this.activeMemTable = activeMemTable;
        this.memTableLock = memTableLock;
        this.wal = wal;
        this.recovering = recovering;
    }

    /**
     * Updates the dependencies of this PutStore.
     * This is useful when the state of the store changes.
     * 
     * @param activeMemTable the new active MemTable
     * @param memTableLock the lock for synchronizing access to the MemTables
     * @param wal the Write-Ahead Log
     * @param recovering flag indicating if the store is in recovery mode
     */
    public void updateDependencies(
            MemTable activeMemTable,
            ReadWriteLock memTableLock,
            WAL wal,
            AtomicBoolean recovering) {
        if (activeMemTable == null) throw new IllegalArgumentException("activeMemTable cannot be null");
        if (memTableLock == null) throw new IllegalArgumentException("memTableLock cannot be null");
        if (wal == null) throw new IllegalArgumentException("wal cannot be null");
        if (recovering == null) throw new IllegalArgumentException("recovering cannot be null");

        this.activeMemTable = activeMemTable;
        this.memTableLock = memTableLock;
        this.wal = wal;
        this.recovering = recovering;
    }

    /**
     * Gets the last exception that occurred during a WAL operation.
     * 
     * @return the last exception, or null if no exception occurred
     */
    public IOException getLastException() {
        return lastException;
    }

    /**
     * Puts a key-value pair into the store with a TTL.
     * 
     * @param key the key as byte array
     * @param value the value as byte array
     * @param ttlSeconds time-to-live in seconds, 0 means no expiration
     * @return the result code of the operation
     */
    public int put(byte[] key, byte[] value, long ttlSeconds) {
        // Reset last exception
        lastException = null;

        // Validate input
        if (key == null || key.length == 0 || value == null) {
            return RESULT_INVALID_INPUT;
        }

        try {
            // Log the operation to WAL first (unless we're recovering)
            if (!recovering.get()) {
                wal.appendPutRecord(key, value, ttlSeconds);
            }

            // Try to put in the active MemTable
            return putInMemTable(key, value, ttlSeconds);
        } catch (IOException e) {
            logger.error("Error writing to WAL", e);
            lastException = e;
            return RESULT_WAL_ERROR;
        }
    }

    /**
     * Attempts to put a key-value pair in the active MemTable.
     * 
     * @param key the key to put
     * @param value the value to put
     * @param ttlSeconds time-to-live in seconds (0 for no expiration)
     * @return the result code of the operation
     */
    private int putInMemTable(byte[] key, byte[] value, long ttlSeconds) {
        boolean success = false;
        boolean needSwitch = false;

        // Acquire read lock for reading from MemTable
        memTableLock.readLock().lock();
        try {
            // Try to put in the active MemTable
            success = activeMemTable.put(key, value, ttlSeconds);

            // If the MemTable is full, we need to switch to a new one
            if (!success && activeMemTable.isFull()) {
                needSwitch = true;
            }
        } finally {
            memTableLock.readLock().unlock();
        }

        // If we need to switch MemTables, signal that to the caller
        if (needSwitch) {
            return RESULT_MEMTABLE_FULL;
        }

        return success ? RESULT_SUCCESS : RESULT_INVALID_INPUT;
    }
}
