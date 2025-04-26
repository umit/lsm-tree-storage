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

package com.umitunal.lsm.core.backgroundservice;

import com.umitunal.lsm.api.ByteArrayWrapper;
import com.umitunal.lsm.config.LSMStoreConfig;
import com.umitunal.lsm.memtable.MemTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Background service that removes expired entries based on TTL.
 * This service scans the active MemTable and immutable MemTables for expired entries
 * and removes them. It also adds tombstone markers for expired entries to ensure
 * they are properly removed during compaction.
 */
public class CleanupService extends AbstractBackgroundService {

    private final MemTable activeMemTable;
    private final List<MemTable> immutableMemTables;
    private final ReadWriteLock memTableLock;
    private final LSMStoreConfig config;

    /**
     * Creates a new cleanup service.
     * 
     * @param activeMemTable the active MemTable to check for expired entries
     * @param immutableMemTables the list of immutable MemTables to check for expired entries
     * @param memTableLock lock for synchronizing access to the MemTables
     * @param config the LSM store configuration
     */
    public CleanupService(MemTable activeMemTable, List<MemTable> immutableMemTables, 
                          ReadWriteLock memTableLock, LSMStoreConfig config) {
        super("TTL-Cleanup");
        this.activeMemTable = activeMemTable;
        this.immutableMemTables = immutableMemTables;
        this.memTableLock = memTableLock;
        this.config = config;
    }

    @Override
    public void start() {
        scheduleTask(1, config.cleanupIntervalMinutes(), TimeUnit.MINUTES);
    }

    @Override
    protected void doExecute() {
        try {
            long now = System.currentTimeMillis();
            List<byte[]> keysToRemove = new ArrayList<>();

            // Check active MemTable for expired entries
            memTableLock.readLock().lock();
            try {
                Map<ByteArrayWrapper, MemTable.ValueEntry> entries = activeMemTable.getEntries();
                for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : entries.entrySet()) {
                    if (entry.getValue().isExpired()) {
                        keysToRemove.add(entry.getKey().getData());
                    }
                }
            } finally {
                memTableLock.readLock().unlock();
            }

            // Remove expired entries from active MemTable
            if (!keysToRemove.isEmpty()) {
                memTableLock.writeLock().lock();
                try {
                    for (byte[] key : keysToRemove) {
                        // Add a tombstone marker to indicate the key is deleted
                        activeMemTable.delete(key);
                        logger.debug("Removed expired entry with key: " + Arrays.toString(key));
                    }
                } finally {
                    memTableLock.writeLock().unlock();
                }
            }

            // Check immutable MemTables for expired entries
            // We don't modify immutable MemTables, but we can add tombstone markers to the active MemTable
            synchronized (immutableMemTables) {
                for (MemTable memTable : immutableMemTables) {
                    keysToRemove.clear();
                    Map<ByteArrayWrapper, MemTable.ValueEntry> entries = memTable.getEntries();
                    for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : entries.entrySet()) {
                        if (entry.getValue().isExpired()) {
                            keysToRemove.add(entry.getKey().getData());
                        }
                    }

                    // Add tombstone markers for expired entries
                    if (!keysToRemove.isEmpty()) {
                        memTableLock.writeLock().lock();
                        try {
                            for (byte[] key : keysToRemove) {
                                // Add a tombstone marker to the active MemTable
                                activeMemTable.delete(key);
                                logger.debug("Added tombstone for expired entry with key: " + Arrays.toString(key));
                            }
                        } finally {
                            memTableLock.writeLock().unlock();
                        }
                    }
                }
            }

            // We don't need to check SSTables for expired entries here
            // Expired entries in SSTables will be filtered out during reads and removed during compaction

            logger.debug("Expired entries cleanup completed");
        } catch (Exception e) {
            logger.warn("Error during expired entries cleanup", e);
        }
    }
}
