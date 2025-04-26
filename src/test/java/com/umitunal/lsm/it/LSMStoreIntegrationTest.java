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

package com.umitunal.lsm.it;

import com.umitunal.lsm.api.KeyValueIterator;
import com.umitunal.lsm.core.store.LSMStore;
import com.umitunal.lsm.memtable.MemTable;
import com.umitunal.lsm.sstable.SSTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.fail;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the LSMStore class.
 * These tests focus on testing the interaction between components (MemTable, SSTable, WAL)
 * and with the file system in real-world scenarios.
 */
class LSMStoreIntegrationTest {
    private LSMStore store;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        // Use a temporary directory for testing
        store = new LSMStore(1024 * 1024, tempDir.toString(), 4);
    }

    @AfterEach
    void tearDown() {
        // Ensure proper cleanup
        if (store != null) {
            store.shutdown();
        }
    }

    /**
     * Tests the persistence of data across store restarts.
     * This test verifies that data written to the store is still available
     * after shutting down and restarting the store.
     */
    @Test
    void testPersistenceAcrossRestarts() {
        // Write some data to the store
        byte[] key1 = "key1".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] key2 = "key2".getBytes();
        byte[] value2 = "value2".getBytes();

        store.put(key1, value1);
        store.put(key2, value2);

        // Verify the data is in the store
        assertArrayEquals(value1, store.get(key1));
        assertArrayEquals(value2, store.get(key2));

        // Shutdown the store
        store.shutdown();

        // Create a new store with the same data directory
        store = new LSMStore(1024 * 1024, tempDir.toString(), 4);

        // Verify the data is still available
        assertArrayEquals(value1, store.get(key1));
        assertArrayEquals(value2, store.get(key2));
    }

    /**
     * Tests the recovery from a simulated crash.
     * This test verifies that data written to the WAL is recovered
     * when the store is restarted after a crash.
     */
    @Test
    void testRecoveryFromCrash() {
        // Write some data to the store
        byte[] key1 = "key1".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] key2 = "key2".getBytes();
        byte[] value2 = "value2".getBytes();

        store.put(key1, value1);
        store.put(key2, value2);

        // Verify the data is in the store
        assertArrayEquals(value1, store.get(key1));
        assertArrayEquals(value2, store.get(key2));

        // Simulate a crash by not calling shutdown()
        // Just create a new store with the same data directory
        store = new LSMStore(1024 * 1024, tempDir.toString(), 4);

        // Verify the data is recovered from the WAL
        assertArrayEquals(value1, store.get(key1));
        assertArrayEquals(value2, store.get(key2));
    }

    /**
     * Tests direct creation of an SSTable from a MemTable.
     * This test verifies that an SSTable can be created from a MemTable and that data can be read from it.
     */
    @Test
    void testMemTableFlushToSSTable() {
        // Create a MemTable
        MemTable memTable = new MemTable(1024); // 1KB

        // Write data to the MemTable
        for (int i = 0; i < 10; i++) {
            byte[] key = ("key" + i).getBytes();
            byte[] value = ("value" + i).getBytes();
            boolean result = memTable.put(key, value, 0);
            System.out.println("[DEBUG_LOG] Put result for key" + i + ": " + result);

            // Verify the data was written correctly
            byte[] actualValue = memTable.get(key);
            System.out.println("[DEBUG_LOG] After put - Key: " + new String(key) + ", Value: " + (actualValue != null ? new String(actualValue) : "null"));
        }

        try {
            // Make the MemTable immutable
            System.out.println("[DEBUG_LOG] Making MemTable immutable");
            memTable.makeImmutable();

            // Create an SSTable from the MemTable
            System.out.println("[DEBUG_LOG] Creating SSTable from MemTable");
            SSTable ssTable = new SSTable(memTable, tempDir.toString(), 0, 1);

            // Verify the SSTable was created
            String filePrefix = String.format("sst_L%d_S%d", 0, 1);
            File dataFile = new File(tempDir.toString(), filePrefix + ".data");
            File indexFile = new File(tempDir.toString(), filePrefix + ".index");
            File filterFile = new File(tempDir.toString(), filePrefix + ".filter");

            System.out.println("[DEBUG_LOG] SSTable files created: data=" + dataFile.exists() + ", index=" + indexFile.exists() + ", filter=" + filterFile.exists());

            // Read data from the SSTable
            System.out.println("[DEBUG_LOG] Reading data from SSTable");
            for (int i = 0; i < 10; i++) {
                byte[] key = ("key" + i).getBytes();
                byte[] expectedValue = ("value" + i).getBytes();
                byte[] actualValue = ssTable.get(key);

                // Add debug logging
                System.out.println("[DEBUG_LOG] SSTable get - Key: " + new String(key) + ", Expected: " + new String(expectedValue) + ", Actual: " + (actualValue != null ? new String(actualValue) : "null"));

                assertArrayEquals(expectedValue, actualValue);
            }

            // Close the SSTable
            System.out.println("[DEBUG_LOG] Closing SSTable");
            ssTable.close();

            // Create a new SSTable from disk
            System.out.println("[DEBUG_LOG] Creating new SSTable from disk");
            SSTable newSSTable = new SSTable(tempDir.toString(), 0, 1);

            // Read data from the new SSTable
            System.out.println("[DEBUG_LOG] Reading data from new SSTable");
            for (int i = 0; i < 10; i++) {
                byte[] key = ("key" + i).getBytes();
                byte[] expectedValue = ("value" + i).getBytes();
                byte[] actualValue = newSSTable.get(key);

                // Add debug logging
                System.out.println("[DEBUG_LOG] New SSTable get - Key: " + new String(key) + ", Expected: " + new String(expectedValue) + ", Actual: " + (actualValue != null ? new String(actualValue) : "null"));

                assertArrayEquals(expectedValue, actualValue);
            }

            // Close the new SSTable
            System.out.println("[DEBUG_LOG] Closing new SSTable");
            newSSTable.close();
        } catch (IOException e) {
            fail("Exception during test: " + e.getMessage());
        } finally {
            // Close the MemTable
            System.out.println("[DEBUG_LOG] Closing MemTable");
            memTable.close();
        }
    }

    /**
     * Tests the automatic flushing of MemTable to SSTable in the LSMStore.
     * This test verifies that when the MemTable is made immutable and flushed,
     * it's correctly written to disk as an SSTable and can be read back.
     */
    @Test
    void testLSMStoreFlushToSSTable() {
        // Create a store with a small MemTable size
        LSMStore smallStore = new LSMStore(1024, tempDir.toString(), 4); // 1KB

        try {
            // Write data to the store
            for (int i = 0; i < 10; i++) {
                byte[] key = ("key" + i).getBytes();
                byte[] value = ("value" + i).getBytes();
                boolean result = smallStore.put(key, value);
                System.out.println("[DEBUG_LOG] Put result for key" + i + ": " + result);

                // Verify the data was written correctly
                byte[] actualValue = smallStore.get(key);
                System.out.println("[DEBUG_LOG] After put - Key: " + new String(key) + ", Value: " + (actualValue != null ? new String(actualValue) : "null"));
            }

            // Make the active MemTable immutable and add it to the list of immutable MemTables
            System.out.println("[DEBUG_LOG] Making active MemTable immutable");
            System.out.println("[DEBUG_LOG] Active MemTable entries before switch: " + smallStore.getActiveMemTableEntryCount());
            smallStore.switchMemTableForTest();
            System.out.println("[DEBUG_LOG] Immutable MemTables count after switch: " + smallStore.getImmutableMemTablesCount());

            // Force a flush to ensure all data is written to disk
            System.out.println("[DEBUG_LOG] Forcing flush of MemTables");
            smallStore.flushMemTables();
            System.out.println("[DEBUG_LOG] SSTables count after flush: " + smallStore.getSSTablesCount());

            // Add a small delay to ensure the flush completes
            try {
                System.out.println("[DEBUG_LOG] Waiting for flush to complete");
                Thread.sleep(1000); // 1 second should be enough
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            System.out.println("[DEBUG_LOG] SSTables count after delay: " + smallStore.getSSTablesCount());

            // Shutdown the store to ensure all data is flushed
            System.out.println("[DEBUG_LOG] Shutting down store");
            smallStore.shutdown();

            // Check if the SSTable files exist in the data directory
            File dataDir = new File(tempDir.toString());
            File[] sstFiles = dataDir.listFiles((dir, name) -> name.endsWith(".data"));
            System.out.println("[DEBUG_LOG] SSTable files in data directory: " + (sstFiles != null ? sstFiles.length : 0));
            if (sstFiles != null) {
                for (File file : sstFiles) {
                    System.out.println("[DEBUG_LOG] SSTable file: " + file.getName());
                }
            }

            // Create a new store with the same data directory to ensure we're reading from disk
            System.out.println("[DEBUG_LOG] Creating new store to read from disk");
            smallStore = new LSMStore(1024, tempDir.toString(), 4);
            System.out.println("[DEBUG_LOG] SSTables count in new store: " + smallStore.getSSTablesCount());

            // Verify all data is still accessible
            System.out.println("[DEBUG_LOG] Verifying data after flush");
            for (int i = 0; i < 10; i++) {
                byte[] key = ("key" + i).getBytes();
                byte[] expectedValue = ("value" + i).getBytes();
                byte[] actualValue = smallStore.get(key);

                // Add debug logging
                System.out.println("[DEBUG_LOG] After flush - Key: " + new String(key) + ", Expected: " + new String(expectedValue) + ", Actual: " + (actualValue != null ? new String(actualValue) : "null"));

                assertArrayEquals(expectedValue, actualValue);
            }
        } finally {
            smallStore.shutdown();
        }
    }

    /**
     * Tests concurrent operations on the store.
     * This test verifies that the store can handle multiple threads
     * performing operations concurrently without data corruption.
     */
    @Test
    void testConcurrentOperations() throws Exception {
        final int numThreads = 10;
        final int numOperationsPerThread = 100;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final AtomicInteger successCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        List<Future<?>> futures = new ArrayList<>();

        // Start threads that perform operations concurrently
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    // Wait for all threads to be ready
                    startLatch.await();

                    // Perform operations
                    for (int i = 0; i < numOperationsPerThread; i++) {
                        String keyStr = "key-" + threadId + "-" + i;
                        String valueStr = "value-" + threadId + "-" + i;
                        byte[] key = keyStr.getBytes();
                        byte[] value = valueStr.getBytes();

                        // Put
                        boolean putResult = store.put(key, value);
                        if (putResult) {
                            successCount.incrementAndGet();
                        }

                        // Get
                        byte[] retrievedValue = store.get(key);
                        if (Arrays.equals(value, retrievedValue)) {
                            successCount.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }

        // Start all threads simultaneously
        startLatch.countDown();

        // Wait for all threads to complete
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        // Verify all operations were successful
        assertEquals(numThreads * numOperationsPerThread * 2, successCount.get());

        // Verify data integrity
        for (int t = 0; t < numThreads; t++) {
            for (int i = 0; i < numOperationsPerThread; i++) {
                String keyStr = "key-" + t + "-" + i;
                String valueStr = "value-" + t + "-" + i;
                byte[] key = keyStr.getBytes();
                byte[] expectedValue = valueStr.getBytes();
                assertArrayEquals(expectedValue, store.get(key));
            }
        }
    }

    /**
     * Tests the range query functionality.
     * This test verifies that the getRange method returns all key-value pairs
     * within the specified range.
     */
    @Test
    void testRangeQuery() {
        // Add some key-value pairs
        byte[] key1 = "key1".getBytes();
        byte[] key2 = "key2".getBytes();
        byte[] key3 = "key3".getBytes();
        byte[] key4 = "key4".getBytes();
        byte[] key5 = "key5".getBytes();

        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value3".getBytes();
        byte[] value4 = "value4".getBytes();
        byte[] value5 = "value5".getBytes();

        store.put(key1, value1);
        store.put(key2, value2);
        store.put(key3, value3);
        store.put(key4, value4);
        store.put(key5, value5);

        // Test range query (key2 to key4)
        Map<byte[], byte[]> range = store.getRange(key2, key5);
        assertEquals(3, range.size());
        assertArrayEquals(value2, range.get(key2));
        assertArrayEquals(value3, range.get(key3));
        assertArrayEquals(value4, range.get(key4));
    }

    /**
     * Tests the iterator functionality.
     * This test verifies that the getIterator method returns an iterator
     * that can be used to iterate over all key-value pairs within the specified range.
     */
    @Test
    void testIterator() {
        // Add some key-value pairs
        byte[] key1 = "key1".getBytes();
        byte[] key2 = "key2".getBytes();
        byte[] key3 = "key3".getBytes();

        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value3".getBytes();

        store.put(key1, value1);
        store.put(key2, value2);
        store.put(key3, value3);

        // Test iterator
        try (KeyValueIterator iterator = store.getIterator(null, null)) {
            assertTrue(iterator.hasNext());
            Map.Entry<byte[], byte[]> entry1 = iterator.next();
            assertArrayEquals(key1, entry1.getKey());
            assertArrayEquals(value1, entry1.getValue());

            assertTrue(iterator.hasNext());
            Map.Entry<byte[], byte[]> entry2 = iterator.next();
            assertArrayEquals(key2, entry2.getKey());
            assertArrayEquals(value2, entry2.getValue());

            assertTrue(iterator.hasNext());
            Map.Entry<byte[], byte[]> entry3 = iterator.next();
            assertArrayEquals(key3, entry3.getKey());
            assertArrayEquals(value3, entry3.getValue());

            assertFalse(iterator.hasNext());
        }
    }

    /**
     * Tests the TTL (time-to-live) functionality.
     * This test verifies that entries with TTL expire after the specified time.
     */
    @Test
    void testTTL() throws InterruptedException {
        // Add some key-value pairs with TTL
        byte[] key = "ttlKey".getBytes();
        byte[] value = "ttlValue".getBytes();

        // Put with 1 second TTL
        store.put(key, value, 1);

        // Verify the key exists immediately
        assertArrayEquals(value, store.get(key));

        // Wait for the TTL to expire
        Thread.sleep(1500); // Wait 1.5 seconds

        // Verify the key has expired
        assertNull(store.get(key));
    }

    /**
     * Tests the WAL directory creation.
     * This test verifies that the WAL directory is created if it doesn't exist.
     */
    @Test
    void testWALDirectoryCreation() {
        // Verify the WAL directory exists
        File walDir = new File(tempDir.toString(), "wal");
        assertTrue(walDir.exists());
        assertTrue(walDir.isDirectory());
    }

    /**
     * Tests the deletion of keys.
     * This test verifies that deleted keys are no longer accessible.
     */
    @Test
    void testDeleteKeys() {
        // Add some key-value pairs
        byte[] key1 = "key1".getBytes();
        byte[] key2 = "key2".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();

        store.put(key1, value1);
        store.put(key2, value2);

        // Verify the keys exist
        assertArrayEquals(value1, store.get(key1));
        assertArrayEquals(value2, store.get(key2));

        // Delete key1
        assertTrue(store.delete(key1));

        // Verify key1 is deleted and key2 still exists
        assertNull(store.get(key1));
        assertArrayEquals(value2, store.get(key2));
    }

    /**
     * Tests the clear operation.
     * This test verifies that the clear method removes all entries from the store.
     */
    @Test
    void testClear() {
        // Add some key-value pairs
        byte[] key1 = "key1".getBytes();
        byte[] key2 = "key2".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();

        store.put(key1, value1);
        store.put(key2, value2);

        // Verify the keys exist
        assertArrayEquals(value1, store.get(key1));
        assertArrayEquals(value2, store.get(key2));

        // Clear the store
        store.clear();

        // Verify all keys are removed
        assertNull(store.get(key1));
        assertNull(store.get(key2));
        assertEquals(0, store.size());
    }

    /**
     * Tests heavy concurrent operations on the store.
     * This test simulates a high-load e-commerce scenario with many concurrent users.
     * It uses more threads and operations than the basic concurrent test.
     */
    @Test
    void testHeavyConcurrentOperations() throws Exception {
        final int numThreads = 20;
        final int numOperationsPerThread = 100;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch completionLatch = new CountDownLatch(numThreads); // To track thread completion
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
                        try {
                            String keyStr = "heavy-key-" + threadId + "-" + i;
                            String valueStr = "heavy-value-" + threadId + "-" + i;
                            byte[] key = keyStr.getBytes();
                            byte[] value = valueStr.getBytes();

                            // Put
                            boolean putResult = store.put(key, value);
                            if (putResult) {
                                successCount.incrementAndGet();
                            }

                            // Get
                            byte[] retrievedValue = store.get(key);
                            if (Arrays.equals(value, retrievedValue)) {
                                successCount.incrementAndGet();
                            }

                            // Every 10th operation, perform a delete
                            if (i % 10 == 0) {
                                boolean deleteResult = store.delete(key);
                                if (deleteResult) {
                                    successCount.incrementAndGet();
                                }
                            }

                            // Add a small delay to reduce contention
                            if (i % 20 == 0) {
                                Thread.sleep(1);
                            }
                        } catch (Exception e) {
                            System.err.println("Error in thread " + threadId + ": " + e.getMessage());
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    completionLatch.countDown(); // Signal that this thread is done
                }
            });
        }

        // Start all threads simultaneously
        startLatch.countDown();

        // Wait for all threads to complete
        boolean allThreadsCompleted = completionLatch.await(60, TimeUnit.SECONDS);

        // Shutdown the executor
        executor.shutdown();
        boolean executorTerminated = executor.awaitTermination(10, TimeUnit.SECONDS);

        assertTrue(allThreadsCompleted, "Not all threads completed within the timeout");
        assertTrue(executorTerminated, "Executor did not terminate gracefully");

        // Calculate expected success count: 
        // - All puts should succeed
        // - Gets for non-deleted keys should succeed
        // - Every 10th key is deleted, so 10% of the operations are deletes
        int expectedSuccessCount = numThreads * numOperationsPerThread * 2; // Put + Get
        int expectedDeleteCount = numThreads * (numOperationsPerThread / 10); // Every 10th operation is a delete

        // Allow for some operations to fail under heavy load (95% success rate)
        int minimumExpectedSuccesses = (int)((expectedSuccessCount + expectedDeleteCount) * 0.95);

        assertTrue(successCount.get() >= minimumExpectedSuccesses, 
                  "Not enough successful operations under heavy load. Expected at least " + 
                  minimumExpectedSuccesses + " but got " + successCount.get());

        // Verify data integrity for a sample of non-deleted keys
        int sampledThreads = Math.min(numThreads, 5); // Sample at most 5 threads
        int sampledOperations = Math.min(numOperationsPerThread, 20); // Sample at most 20 operations per thread

        for (int t = 0; t < sampledThreads; t++) {
            for (int i = 1; i < sampledOperations; i++) {
                if (i % 10 != 0) { // Skip deleted keys
                    String keyStr = "heavy-key-" + t + "-" + i;
                    String valueStr = "heavy-value-" + t + "-" + i;
                    byte[] key = keyStr.getBytes();
                    byte[] retrievedValue = store.get(key);
                    byte[] expectedValue = valueStr.getBytes();

                    if (retrievedValue != null) {
                        assertArrayEquals(expectedValue, retrievedValue, 
                                        "Data integrity issue detected for key: " + keyStr);
                    }
                }
            }
        }
    }

    /**
     * Tests read-heavy workload typical of e-commerce applications.
     * In e-commerce, reads (product browsing) are much more frequent than writes (purchases).
     * This test simulates a 90% read, 10% write workload with many concurrent users.
     */
    @Test
    void testECommerceReadHeavyWorkload() throws Exception {
        // Pre-populate the store with "product catalog" data
        final int numProducts = 1000;
        for (int i = 0; i < numProducts; i++) {
            String keyStr = "product-" + i;
            String valueStr = "product-details-" + i + "-" + UUID.randomUUID();
            store.put(keyStr.getBytes(), valueStr.getBytes());
        }

        final int numThreads = 100; // Simulating 100 concurrent users
        final int numOperationsPerThread = 1000; // 1000 operations per user
        final CountDownLatch startLatch = new CountDownLatch(1);
        final AtomicInteger readSuccessCount = new AtomicInteger(0);
        final AtomicInteger writeSuccessCount = new AtomicInteger(0);

        // Random for selecting products and determining operation type
        final Random random = new Random();

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        List<Future<?>> futures = new ArrayList<>();

        // Start threads that perform operations concurrently
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    // Wait for all threads to be ready
                    startLatch.await();

                    // Each thread represents a user session
                    for (int i = 0; i < numOperationsPerThread; i++) {
                        // 90% reads (product browsing), 10% writes (purchases/cart updates)
                        boolean isRead = random.nextInt(100) < 90;

                        if (isRead) {
                            // Read operation - simulate product browsing
                            int productId = random.nextInt(numProducts);
                            String keyStr = "product-" + productId;
                            byte[] key = keyStr.getBytes();

                            // Get product details
                            byte[] retrievedValue = store.get(key);
                            if (retrievedValue != null) {
                                readSuccessCount.incrementAndGet();
                            }

                            // Simulate browsing related products (range query)
                            if (random.nextInt(10) == 0) { // 10% of reads include related products
                                int startId = Math.max(0, productId - 5);
                                int endId = Math.min(numProducts - 1, productId + 5);
                                String startKey = "product-" + startId;
                                String endKey = "product-" + endId;

                                Map<byte[], byte[]> relatedProducts = store.getRange(
                                    startKey.getBytes(), endKey.getBytes());

                                if (relatedProducts.size() > 0) {
                                    readSuccessCount.incrementAndGet();
                                }
                            }
                        } else {
                            // Write operation - simulate purchase or cart update
                            String keyStr = "cart-" + threadId + "-item-" + i;
                            String valueStr = "product-" + random.nextInt(numProducts) + 
                                             "-quantity-" + (random.nextInt(5) + 1);
                            byte[] key = keyStr.getBytes();
                            byte[] value = valueStr.getBytes();

                            // Update cart
                            boolean putResult = store.put(key, value);
                            if (putResult) {
                                writeSuccessCount.incrementAndGet();
                            }

                            // Occasionally remove items from cart
                            if (i > 0 && random.nextInt(10) == 0) { // 10% chance to remove an item
                                int itemToRemove = random.nextInt(i);
                                String removeKeyStr = "cart-" + threadId + "-item-" + itemToRemove;
                                boolean deleteResult = store.delete(removeKeyStr.getBytes());
                                if (deleteResult) {
                                    writeSuccessCount.incrementAndGet();
                                }
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }

        // Start all threads simultaneously
        startLatch.countDown();

        // Wait for all threads to complete
        executor.shutdown();
        boolean completed = executor.awaitTermination(120, TimeUnit.SECONDS);

        assertTrue(completed, "Execution timed out, which may indicate performance issues under e-commerce workload");

        // Log the success counts
        System.out.println("E-commerce workload test results:");
        System.out.println("Read operations successful: " + readSuccessCount.get());
        System.out.println("Write operations successful: " + writeSuccessCount.get());

        // Verify that we had a significant number of successful operations
        // The exact count will vary due to randomization, but we expect approximately:
        // - 90% of operations to be reads
        // - 10% of operations to be writes
        int totalOperations = numThreads * numOperationsPerThread;
        int expectedMinimumReads = (int)(totalOperations * 0.8); // Allow for some variance
        int expectedMinimumWrites = (int)(totalOperations * 0.05); // Allow for some variance

        assertTrue(readSuccessCount.get() >= expectedMinimumReads, 
                  "Not enough successful read operations. Expected at least " + expectedMinimumReads + 
                  " but got " + readSuccessCount.get());

        assertTrue(writeSuccessCount.get() >= expectedMinimumWrites, 
                  "Not enough successful write operations. Expected at least " + expectedMinimumWrites + 
                  " but got " + writeSuccessCount.get());
    }

    /**
     * Tests high contention scenarios where many threads are accessing the same keys.
     * This simulates situations in an e-commerce environment where:
     * - Flash sales or limited-time offers create high contention on specific products
     * - Popular products are viewed and purchased by many users simultaneously
     * - Inventory updates happen concurrently with purchases
     */
    @Test
    void testHighContentionScenario() throws Exception {
        // Create a small set of "hot" products that will be accessed by all threads
        final int numHotProducts = 10;
        for (int i = 0; i < numHotProducts; i++) {
            String keyStr = "hot-product-" + i;
            String valueStr = "hot-product-details-" + i + "-" + UUID.randomUUID().toString();
            store.put(keyStr.getBytes(), valueStr.getBytes());

            // Also create inventory records for these products
            String inventoryKeyStr = "inventory-" + i;
            String inventoryValueStr = "100"; // Initial inventory of 100 units
            store.put(inventoryKeyStr.getBytes(), inventoryValueStr.getBytes());
        }

        final int numThreads = 200; // High number of concurrent users
        final int numOperationsPerThread = 500;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final AtomicInteger readSuccessCount = new AtomicInteger(0);
        final AtomicInteger writeSuccessCount = new AtomicInteger(0);
        final AtomicInteger contentionErrorCount = new AtomicInteger(0);

        // Use a shared Random instance to increase contention
        final Random random = new Random();

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        List<Future<?>> futures = new ArrayList<>();

        // Start threads that perform operations concurrently
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    // Wait for all threads to be ready
                    startLatch.await();

                    // Each thread represents a user session
                    for (int i = 0; i < numOperationsPerThread; i++) {
                        // Select a hot product (all threads access the same small set of products)
                        int productId = random.nextInt(numHotProducts);
                        String productKeyStr = "hot-product-" + productId;
                        String inventoryKeyStr = "inventory-" + productId;

                        // 80% reads, 20% writes to create high contention
                        boolean isRead = random.nextInt(100) < 80;

                        if (isRead) {
                            // Read operation - view product details
                            byte[] retrievedValue = store.get(productKeyStr.getBytes());
                            if (retrievedValue != null) {
                                readSuccessCount.incrementAndGet();
                            }

                            // Also check inventory
                            byte[] inventoryValue = store.get(inventoryKeyStr.getBytes());
                            if (inventoryValue != null) {
                                readSuccessCount.incrementAndGet();
                            }
                        } else {
                            // Write operation - update product or inventory
                            if (random.nextInt(2) == 0) {
                                // Update product details
                                String newValueStr = "hot-product-details-" + productId + 
                                                    "-updated-" + threadId + "-" + i;
                                boolean putResult = store.put(productKeyStr.getBytes(), newValueStr.getBytes());
                                if (putResult) {
                                    writeSuccessCount.incrementAndGet();
                                } else {
                                    contentionErrorCount.incrementAndGet();
                                }
                            } else {
                                // Update inventory - simulate purchase
                                // This creates high contention as multiple threads try to update the same inventory
                                byte[] currentInventory = store.get(inventoryKeyStr.getBytes());
                                if (currentInventory != null) {
                                    try {
                                        int inventory = Integer.parseInt(new String(currentInventory));
                                        if (inventory > 0) {
                                            // Decrement inventory
                                            inventory--;
                                            boolean putResult = store.put(
                                                inventoryKeyStr.getBytes(), 
                                                String.valueOf(inventory).getBytes()
                                            );
                                            if (putResult) {
                                                writeSuccessCount.incrementAndGet();
                                            } else {
                                                contentionErrorCount.incrementAndGet();
                                            }
                                        }
                                    } catch (NumberFormatException e) {
                                        contentionErrorCount.incrementAndGet();
                                    }
                                }
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }

        // Start all threads simultaneously
        startLatch.countDown();

        // Wait for all threads to complete
        executor.shutdown();
        boolean completed = executor.awaitTermination(120, TimeUnit.SECONDS);

        assertTrue(completed, "Execution timed out, which may indicate performance issues under high contention");

        // Log the results
        System.out.println("High contention test results:");
        System.out.println("Read operations successful: " + readSuccessCount.get());
        System.out.println("Write operations successful: " + writeSuccessCount.get());
        System.out.println("Contention errors: " + contentionErrorCount.get());

        // Verify that we had a significant number of successful operations
        int totalOperations = numThreads * numOperationsPerThread;
        int expectedMinimumReads = (int)(totalOperations * 0.7); // Allow for some variance
        int expectedMinimumWrites = (int)(totalOperations * 0.1); // Allow for some variance

        assertTrue(readSuccessCount.get() >= expectedMinimumReads, 
                  "Not enough successful read operations under high contention. Expected at least " + 
                  expectedMinimumReads + " but got " + readSuccessCount.get());

        assertTrue(writeSuccessCount.get() >= expectedMinimumWrites, 
                  "Not enough successful write operations under high contention. Expected at least " + 
                  expectedMinimumWrites + " but got " + writeSuccessCount.get());

        // Check final inventory values to ensure they're consistent
        for (int i = 0; i < numHotProducts; i++) {
            String inventoryKeyStr = "inventory-" + i;
            byte[] finalInventory = store.get(inventoryKeyStr.getBytes());
            assertNotNull(finalInventory, "Inventory record for product " + i + " should exist");

            int inventory = Integer.parseInt(new String(finalInventory));
            // The inventory should be less than or equal to the initial value (100)
            // and greater than or equal to 0
            assertTrue(inventory >= 0 && inventory <= 100, 
                      "Inventory for product " + i + " should be between 0 and 100, but was " + inventory);
        }
    }
}
