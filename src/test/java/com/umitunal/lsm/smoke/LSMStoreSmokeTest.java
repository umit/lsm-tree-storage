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

package com.umitunal.lsm.smoke;

import com.umitunal.lsm.api.KeyValueIterator;
import com.umitunal.lsm.core.store.LSMStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Smoke tests for the LSMStore implementation.
 * These tests quickly validate the basic functionality and stability of the system
 * without going into detailed testing. They're useful for quick validation before
 * running more comprehensive tests.
 */
public class LSMStoreSmokeTest {
    private LSMStore store;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        // Use a temporary directory for testing with a small MemTable size to force flushing
        store = new LSMStore(64 * 1024, tempDir.toString(), 2);
    }

    @AfterEach
    void tearDown() {
        // Ensure proper cleanup
        if (store != null) {
            store.shutdown();
        }
    }

    /**
     * Basic smoke test that verifies the core operations of the LSMStore.
     * This test should run quickly and validate that the system is stable.
     */
    @Test
    void basicSmokeTest() {
        // Test put and get
        byte[] key1 = "smoke-key1".getBytes();
        byte[] value1 = "smoke-value1".getBytes();
        assertTrue(store.put(key1, value1), "Put operation should succeed");
        assertArrayEquals(value1, store.get(key1), "Get operation should return the correct value");

        // Test delete
        assertTrue(store.delete(key1), "Delete operation should succeed");
        assertNull(store.get(key1), "Get operation should return null after delete");

        // Test put with TTL
        byte[] key2 = "smoke-key2".getBytes();
        byte[] value2 = "smoke-value2".getBytes();
        assertTrue(store.put(key2, value2, 1), "Put with TTL operation should succeed");
        assertArrayEquals(value2, store.get(key2), "Get operation should return the correct value before TTL expires");

        // Wait for TTL to expire
        try {
            Thread.sleep(1500); // Wait 1.5 seconds
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertNull(store.get(key2), "Get operation should return null after TTL expires");

        // Test range operations
        for (int i = 0; i < 10; i++) {
            byte[] key = ("smoke-range-key" + i).getBytes();
            byte[] value = ("smoke-range-value" + i).getBytes();
            assertTrue(store.put(key, value), "Put operation should succeed for range test");
        }

        // Test range query
        Map<byte[], byte[]> range = store.getRange("smoke-range-key3".getBytes(), "smoke-range-key7".getBytes());
        assertEquals(4, range.size(), "Range query should return the correct number of entries");

        // Test iterator
        try (KeyValueIterator iterator = store.getIterator("smoke-range-key3".getBytes(), "smoke-range-key7".getBytes())) {
            int count = 0;
            while (iterator.hasNext()) {
                Map.Entry<byte[], byte[]> entry = iterator.next();
                assertNotNull(entry.getKey(), "Iterator entry key should not be null");
                assertNotNull(entry.getValue(), "Iterator entry value should not be null");
                count++;
            }
            assertEquals(4, count, "Iterator should return the correct number of entries");
        }
    }

    /**
     * Quick concurrent operations smoke test.
     * This test verifies that the system can handle a moderate level of concurrency
     * without errors or deadlocks.
     */
    @Test
    void concurrentOperationsSmokeTest() throws Exception {
        final int numThreads = 10;
        final int numOperationsPerThread = 100;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final AtomicInteger successCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        // Start threads that perform operations concurrently
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    // Wait for all threads to be ready
                    startLatch.await();

                    // Perform operations
                    for (int i = 0; i < numOperationsPerThread; i++) {
                        String keyStr = "smoke-concurrent-key-" + threadId + "-" + i;
                        String valueStr = "smoke-concurrent-value-" + threadId + "-" + i;
                        byte[] key = keyStr.getBytes();
                        byte[] value = valueStr.getBytes();

                        // Put
                        boolean putResult = store.put(key, value);
                        if (putResult) {
                            successCount.incrementAndGet();
                        }

                        // Get
                        byte[] retrievedValue = store.get(key);
                        if (retrievedValue != null && new String(retrievedValue).equals(valueStr)) {
                            successCount.incrementAndGet();
                        }

                        // Every 10th operation, perform a delete
                        if (i % 10 == 0) {
                            boolean deleteResult = store.delete(key);
                            if (deleteResult) {
                                successCount.incrementAndGet();
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // Start all threads simultaneously
        startLatch.countDown();

        // Wait for all threads to complete
        executor.shutdown();
        boolean completed = executor.awaitTermination(30, TimeUnit.SECONDS);

        assertTrue(completed, "Execution timed out, which may indicate stability issues");

        // Calculate expected success count
        int expectedPutSuccesses = numThreads * numOperationsPerThread;
        int expectedGetSuccesses = numThreads * numOperationsPerThread;
        int expectedDeleteSuccesses = numThreads * (numOperationsPerThread / 10);
        int expectedTotalSuccesses = expectedPutSuccesses + expectedGetSuccesses + expectedDeleteSuccesses;

        // Allow for some variance in success count
        int minimumExpectedSuccesses = (int)(expectedTotalSuccesses * 0.9);

        assertTrue(successCount.get() >= minimumExpectedSuccesses, 
                  "Not enough successful operations. Expected at least " + minimumExpectedSuccesses + 
                  " but got " + successCount.get());
    }

    /**
     * Quick persistence smoke test.
     * This test verifies that data is persisted across store restarts.
     */
    @Test
    void persistenceSmokeTest() {
        // Write some data to the store
        byte[] key = "smoke-persistence-key".getBytes();
        byte[] value = "smoke-persistence-value".getBytes();
        assertTrue(store.put(key, value), "Put operation should succeed");
        assertArrayEquals(value, store.get(key), "Get operation should return the correct value");

        // Shutdown the store
        store.shutdown();

        // Create a new store with the same data directory
        store = new LSMStore(64 * 1024, tempDir.toString(), 2);

        // Verify the data is still available
        assertArrayEquals(value, store.get(key), "Data should be persisted across store restarts");
    }
}
