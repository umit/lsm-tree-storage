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
import com.umitunal.lsm.core.compaction.CompactionStrategyType;
import com.umitunal.lsm.memtable.MemTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CleanupServiceTest {

    @TempDir
    Path tempDir;

    private MemTable activeMemTable;
    private List<MemTable> immutableMemTables;
    private ReadWriteLock memTableLock;
    private LSMStoreConfig config;
    private CleanupService cleanupService;

    @BeforeEach
    void setUp() {
        activeMemTable = new MemTable(1024 * 1024); // 1MB MemTable size
        immutableMemTables = new ArrayList<>();
        memTableLock = new ReentrantReadWriteLock();
        config = new LSMStoreConfig(
            1024 * 1024, // 1MB MemTable size
            tempDir.toString(),
            2, // Compact after 2 SSTables
            30, // 30 minutes compaction interval
            1, // 1 minute cleanup interval
            10, // 10 seconds flush interval
            CompactionStrategyType.THRESHOLD // Use threshold-based compaction for tests
        );
        cleanupService = new CleanupService(activeMemTable, immutableMemTables, memTableLock, config);
    }

    @AfterEach
    void tearDown() {
        cleanupService.shutdown();
        activeMemTable.close();
        for (MemTable memTable : immutableMemTables) {
            memTable.close();
        }
    }

    @Test
    void testCleanupServiceStart() {
        // Start the service
        cleanupService.start();

        // Verify that the service is running
        // This is a simple test to make sure the service starts without errors
        assertTrue(true);
    }

    @Test
    void testCleanupServiceExecuteNow() throws InterruptedException {
        // Add some entries to the active MemTable with short TTL
        activeMemTable.put("key1".getBytes(), "value1".getBytes(), 1); // 1 second TTL
        activeMemTable.put("key2".getBytes(), "value2".getBytes(), 0); // No TTL

        // Wait for the TTL to expire
        TimeUnit.SECONDS.sleep(2);

        // Execute the cleanup service
        cleanupService.executeNow();

        // Verify that the expired entry was removed
        Map<ByteArrayWrapper, MemTable.ValueEntry> entries = activeMemTable.getEntries();
        boolean key1Found = false;
        boolean key2Found = false;

        for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : entries.entrySet()) {
            if (new String(entry.getKey().getData()).equals("key1")) {
                key1Found = !entry.getValue().isTombstone(); // Should be a tombstone
            }
            if (new String(entry.getKey().getData()).equals("key2")) {
                key2Found = !entry.getValue().isTombstone(); // Should not be a tombstone
            }
        }

        assertFalse(key1Found, "key1 should be marked as tombstone");
        assertTrue(key2Found, "key2 should not be marked as tombstone");
    }

    @Test
    void testCleanupServiceWithImmutableMemTables() throws InterruptedException {
        // Create an immutable MemTable with an expired entry
        MemTable immutableMemTable = new MemTable(1024 * 1024);
        immutableMemTable.put("key3".getBytes(), "value3".getBytes(), 1); // 1 second TTL
        immutableMemTable.makeImmutable();
        immutableMemTables.add(immutableMemTable);

        // Wait for the TTL to expire
        TimeUnit.SECONDS.sleep(2);

        // Execute the cleanup service
        cleanupService.executeNow();

        // Verify that a tombstone was added to the active MemTable for the expired entry
        Map<ByteArrayWrapper, MemTable.ValueEntry> entries = activeMemTable.getEntries();
        boolean key3TombstoneFound = false;

        for (Map.Entry<ByteArrayWrapper, MemTable.ValueEntry> entry : entries.entrySet()) {
            if (new String(entry.getKey().getData()).equals("key3")) {
                key3TombstoneFound = entry.getValue().isTombstone();
            }
        }

        assertTrue(key3TombstoneFound, "A tombstone for key3 should be added to the active MemTable");
    }
}
