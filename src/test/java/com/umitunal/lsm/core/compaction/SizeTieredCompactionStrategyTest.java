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

package com.umitunal.lsm.core.compaction;

import com.umitunal.lsm.config.LSMStoreConfig;
import com.umitunal.lsm.memtable.MemTable;
import com.umitunal.lsm.sstable.SSTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class SizeTieredCompactionStrategyTest {

    @TempDir
    Path tempDir;

    private LSMStoreConfig config;
    private SizeTieredCompactionStrategy strategy;
    private List<SSTable> ssTables;
    private AtomicLong sequenceNumber;

    @BeforeEach
    void setUp() {
        config = new LSMStoreConfig(
            1024 * 1024, // 1MB MemTable size
            tempDir.toString(),
            4, // Compact after 4 SSTables
            30, // 30 minutes compaction interval
            1, // 1 minute cleanup interval
            10, // 10 seconds flush interval
            CompactionStrategyType.SIZE_TIERED
        );
        strategy = new SizeTieredCompactionStrategy(config);
        ssTables = new ArrayList<>();
        sequenceNumber = new AtomicLong(0);
    }

    @AfterEach
    void tearDown() {
        for (SSTable ssTable : ssTables) {
            try {
                ssTable.close();
                ssTable.delete();
            } catch (Exception e) {
                // Ignore
            }
        }
    }

    @Test
    void testShouldCompactWithNotEnoughSSTables() {
        // Test with empty list
        assertFalse(strategy.shouldCompact(ssTables));

        // Test with fewer than minThreshold SSTables
        try {
            // Create 3 SSTables (below the default minThreshold of 4)
            createSSTables(3, 1024); // All same size
            assertFalse(strategy.shouldCompact(ssTables));
        } catch (IOException e) {
            fail("Failed to create SSTables: " + e.getMessage());
        }
    }

    @Test
    void testShouldCompactWithEnoughSSTables() {
        try {
            // Create 5 SSTables of similar size (above the default minThreshold of 4)
            createSSTables(5, 1024); // All same size
            assertTrue(strategy.shouldCompact(ssTables));
        } catch (IOException e) {
            fail("Failed to create SSTables: " + e.getMessage());
        }
    }

    @Test
    void testShouldCompactWithDifferentSizeBuckets() {
        try {
            // Create 8 SSTables with different sizes
            // 4 small SSTables (around 1KB)
            createSSTables(4, 1024);
            
            // 4 large SSTables (around 10KB)
            createSSTables(4, 10240);
            
            // Should compact because we have at least one bucket with >= minThreshold SSTables
            assertTrue(strategy.shouldCompact(ssTables));
        } catch (IOException e) {
            fail("Failed to create SSTables: " + e.getMessage());
        }
    }

    @Test
    void testSelectTableToCompactWithNotEnoughSSTables() {
        // Test with empty list
        assertTrue(strategy.selectTableToCompact(ssTables).isEmpty());

        try {
            // Create 3 SSTables (below the default minThreshold of 4)
            createSSTables(3, 1024);
            assertTrue(strategy.selectTableToCompact(ssTables).isEmpty());
        } catch (IOException e) {
            fail("Failed to create SSTables: " + e.getMessage());
        }
    }

    @Test
    void testSelectTableToCompactWithEnoughSSTables() {
        try {
            // Create 5 SSTables of similar size
            createSSTables(5, 1024);
            
            // Should select all 5 SSTables for compaction
            List<SSTable> selected = strategy.selectTableToCompact(ssTables);
            assertEquals(5, selected.size());
        } catch (IOException e) {
            fail("Failed to create SSTables: " + e.getMessage());
        }
    }

    @Test
    void testSelectTableToCompactWithDifferentSizeBuckets() {
        try {
            // Create 4 small SSTables (around 1KB)
            createSSTables(4, 1024);
            
            // Create 5 large SSTables (around 10KB)
            createSSTables(5, 10240);
            
            // Should select the bucket with more SSTables (the 5 large ones)
            List<SSTable> selected = strategy.selectTableToCompact(ssTables);
            assertEquals(5, selected.size());
            
            // Verify that the selected SSTables are the large ones
            for (SSTable ssTable : selected) {
                assertTrue(ssTable.getSizeBytes() > 5000, "Selected SSTable should be from the large bucket");
            }
        } catch (IOException e) {
            fail("Failed to create SSTables: " + e.getMessage());
        }
    }

    @Test
    void testGetCompactionOutputLevel() {
        try {
            // Create 5 SSTables at level 0
            createSSTables(5, 1024, 0);
            
            // Select tables to compact
            List<SSTable> selected = strategy.selectTableToCompact(ssTables);
            
            // Output level should be 1 (max level + 1)
            assertEquals(1, strategy.getCompactionOutputLevel(selected));
            
            // Create 3 SSTables at level 1
            createSSTables(3, 1024, 1);
            
            // Create a mixed list with different levels
            List<SSTable> mixedLevels = new ArrayList<>();
            mixedLevels.add(ssTables.get(0)); // Level 0
            mixedLevels.add(ssTables.get(1)); // Level 0
            mixedLevels.add(ssTables.get(7)); // Level 1
            
            // Output level should be 2 (max level + 1)
            assertEquals(2, strategy.getCompactionOutputLevel(mixedLevels));
        } catch (IOException e) {
            fail("Failed to create SSTables: " + e.getMessage());
        }
    }

    /**
     * Helper method to create SSTables with specified approximate size.
     * 
     * @param count the number of SSTables to create
     * @param approxSizeBytes the approximate size of each SSTable in bytes
     * @throws IOException if an I/O error occurs
     */
    private void createSSTables(int count, int approxSizeBytes) throws IOException {
        createSSTables(count, approxSizeBytes, 0); // Default to level 0
    }

    /**
     * Helper method to create SSTables with specified approximate size and level.
     * 
     * @param count the number of SSTables to create
     * @param approxSizeBytes the approximate size of each SSTable in bytes
     * @param level the level of the SSTables
     * @throws IOException if an I/O error occurs
     */
    private void createSSTables(int count, int approxSizeBytes, int level) throws IOException {
        // Calculate how many entries we need to create an SSTable of the desired size
        // Each entry is roughly 100 bytes (key + value + metadata)
        int entriesPerSSTable = approxSizeBytes / 100;
        
        for (int i = 0; i < count; i++) {
            MemTable memTable = new MemTable(config.memTableMaxSizeBytes());
            
            // Add entries to the MemTable
            for (int j = 0; j < entriesPerSSTable; j++) {
                String key = "key-" + i + "-" + j;
                String value = "value-" + i + "-" + j + "-" + "x".repeat(50); // Pad to make it larger
                memTable.put(key.getBytes(), value.getBytes(), 0);
            }
            
            // Create an SSTable from the MemTable
            SSTable ssTable = new SSTable(memTable, config.dataDirectory(), level, sequenceNumber.getAndIncrement());
            ssTables.add(ssTable);
        }
    }
}