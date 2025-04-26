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

package com.umitunal.lsm;

import com.umitunal.lsm.core.store.LSMStore;
import com.umitunal.lsm.core.compaction.CompactionStrategyType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the LSMStore with the SizeTieredCompactionStrategy.
 * These tests verify that the LSMStore works correctly with the new compaction strategy.
 */
class LSMStoreWithSizeTieredCompactionTest {
    private LSMStore store;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        // Use a temporary directory for testing with a small MemTable size to force frequent flushing
        // and the SIZE_TIERED compaction strategy
        store = new LSMStore(
            32 * 1024, // 32KB MemTable size
            tempDir.toString(),
            4, // Compact after 4 SSTables
            CompactionStrategyType.SIZE_TIERED
        );
    }

    @AfterEach
    void tearDown() {
        // Ensure proper cleanup
        if (store != null) {
            store.shutdown();
        }
    }

    /**
     * Tests that the LSMStore can be created with the SizeTieredCompactionStrategy.
     * This is a basic test to verify that the LSMStore can be initialized with the
     * SizeTieredCompactionStrategy and that basic operations work.
     */
    @Test
    void testSizeTieredCompaction() throws IOException, InterruptedException {
        // Create a small set of key-value pairs
        for (int i = 0; i < 10; i++) {
            String key = "key-" + i;
            String value = "value-" + i;

            // Put the key-value pair
            assertTrue(store.put(key.getBytes(), value.getBytes()));

            // Verify that the key-value pair was stored correctly
            assertArrayEquals(value.getBytes(), store.get(key.getBytes()));
        }

        // Verify that all keys can be retrieved
        for (int i = 0; i < 10; i++) {
            String key = "key-" + i;
            String value = "value-" + i;

            assertArrayEquals(value.getBytes(), store.get(key.getBytes()));
        }
    }

    /**
     * Tests that the LSMStore with SizeTieredCompactionStrategy correctly handles
     * updates to existing keys.
     */
    @Test
    void testUpdatesWithSizeTieredCompaction() throws IOException, InterruptedException {
        // Write initial data
        for (int i = 0; i < 10; i++) {
            String key = "update-key-" + i;
            String value = "initial-value-" + i;
            assertTrue(store.put(key.getBytes(), value.getBytes()));
        }

        // Update the keys with new values
        for (int i = 0; i < 10; i++) {
            String key = "update-key-" + i;
            String value = "updated-value-" + i;
            assertTrue(store.put(key.getBytes(), value.getBytes()));
        }

        // Verify that all keys have their latest values
        for (int i = 0; i < 10; i++) {
            String key = "update-key-" + i;
            String expectedValue = "updated-value-" + i;
            assertArrayEquals(expectedValue.getBytes(), store.get(key.getBytes()),
                             "Key " + key + " should have its updated value");
        }
    }
}
