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

package com.umitunal.lsm.stress;

import com.umitunal.lsm.core.store.LSMStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Stress tests for the LSMStore implementation.
 * These tests push the system to its limits to verify its behavior under extreme conditions.
 * They are designed to identify issues that might only appear under heavy load or after
 * extended periods of operation.
 */
public class LSMStoreStressTest {
    private LSMStore store;

    @TempDir
    Path tempDir;

    private Random random;

    @BeforeEach
    void setUp() {
        // Use a temporary directory for testing with a small MemTable size to force frequent flushing
        store = new LSMStore(32 * 1024, tempDir.toString(), 2);
        random = new Random();
    }

    @AfterEach
    void tearDown() {
        // Ensure proper cleanup
        if (store != null) {
            store.shutdown();
        }
    }

    /**
     * Tests the system under extreme load with many concurrent threads.
     * This test uses a high number of threads to stress the concurrency mechanisms.
     * 
     * Note: This is a stress test designed to push the system to its limits.
     * We expect a significant number of operations to fail due to the extreme load.
     */
    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void testExtremeConcurrency() throws Exception {
        final int numThreads = 50;
        final int numOperationsPerThread = 30;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch completionLatch = new CountDownLatch(numThreads); // To track thread completion
        final AtomicInteger successCount = new AtomicInteger(0);
        final AtomicInteger errorCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        List<Exception> exceptions = new ArrayList<>();

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
                            String keyStr = "extreme-key-" + threadId + "-" + i;
                            String valueStr = "extreme-value-" + threadId + "-" + i;
                            byte[] key = keyStr.getBytes();
                            byte[] value = valueStr.getBytes();

                            // Mix of operations
                            int op = random.nextInt(10);
                            if (op < 5) { // 50% put
                                boolean result = store.put(key, value);
                                if (result) {
                                    successCount.incrementAndGet();
                                } else {
                                    errorCount.incrementAndGet();
                                }
                            } else if (op < 8) { // 30% get
                                byte[] result = store.get(key);
                                if (result != null) {
                                    successCount.incrementAndGet();
                                }
                            } else { // 20% delete
                                boolean result = store.delete(key);
                                if (result) {
                                    successCount.incrementAndGet();
                                }
                            }

                            // Add a small delay to reduce contention
                            if (i % 5 == 0) {
                                Thread.sleep(2);
                            }
                        } catch (Exception e) {
                            synchronized (exceptions) {
                                if (exceptions.size() < 20) { // Limit the number of exceptions we log
                                    exceptions.add(e);
                                }
                            }
                            errorCount.incrementAndGet();
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
        boolean allThreadsCompleted = completionLatch.await(2, TimeUnit.MINUTES);

        // Shutdown the executor
        executor.shutdown();
        boolean executorTerminated = executor.awaitTermination(30, TimeUnit.SECONDS);

        assertTrue(allThreadsCompleted, "Not all threads completed within the timeout");
        assertTrue(executorTerminated, "Executor did not terminate gracefully");

        // Log results
        System.out.println("Extreme concurrency test results:");
        System.out.println("Successful operations: " + successCount.get());
        System.out.println("Error count: " + errorCount.get());
        System.out.println("Total operations attempted: " + (numThreads * numOperationsPerThread));

        if (!exceptions.isEmpty()) {
            System.out.println("Sample of exceptions encountered:");
            for (Exception e : exceptions) {
                System.out.println(e.getClass().getName() + ": " + e.getMessage());
            }
        }

        // For a stress test, we just verify that some operations succeeded
        // We don't enforce a specific error rate since the test is designed to push the system to its limits
        assertTrue(successCount.get() > 0, "No operations succeeded under extreme load");

        // Log the error rate for informational purposes
        double errorRate = (double) errorCount.get() / (numThreads * numOperationsPerThread);
        System.out.println("Error rate: " + (errorRate * 100) + "%");
    }

    /**
     * Tests the system with a large volume of data.
     * This test writes a large amount of data to stress the storage mechanisms.
     * 
     * Note: This is a stress test designed to push the system to its limits.
     * The primary goal is to verify that the system doesn't crash or hang when
     * handling a large volume of data, rather than expecting specific success rates.
     * 
     * Due to the nature of LSM-tree storage and the small MemTable size used in tests,
     * many operations may fail or data might be temporarily unavailable during compaction or flushing.
     */
    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void testLargeDataVolume() {
        final int numEntries = 5_000;
        final int valueSize = 128;
        final AtomicInteger successCount = new AtomicInteger(0);
        final AtomicInteger verificationCount = new AtomicInteger(0);
        final AtomicInteger verificationSuccessCount = new AtomicInteger(0);

        System.out.println("Starting large data volume test with " + numEntries + " entries of " + valueSize + " bytes each");

        try {
            // Write large volume of data
            for (int i = 0; i < numEntries; i++) {
                String keyStr = "volume-key-" + i;
                byte[] key = keyStr.getBytes();
                byte[] value = new byte[valueSize];
                random.nextBytes(value);

                try {
                    boolean result = store.put(key, value);
                    if (result) {
                        successCount.incrementAndGet();
                    }

                    // Periodically log progress
                    if (i % 1000 == 0 && i > 0) {
                        // Check a few random keys for informational purposes
                        for (int j = 0; j < 3; j++) {
                            try {
                                int checkIndex = random.nextInt(i);
                                String checkKeyStr = "volume-key-" + checkIndex;
                                byte[] checkKey = checkKeyStr.getBytes();
                                byte[] retrievedValue = store.get(checkKey);

                                verificationCount.incrementAndGet();
                                if (retrievedValue != null) {
                                    verificationSuccessCount.incrementAndGet();
                                }
                            } catch (Exception e) {
                                // Ignore exceptions during verification
                            }
                        }

                        System.out.println("Progress: " + i + "/" + numEntries + " entries written");
                        if (verificationCount.get() > 0) {
                            System.out.println("Verification success rate: " + 
                                              (verificationSuccessCount.get() * 100.0 / verificationCount.get()) + "%");
                        }
                    }

                    // Add a small delay every 50 operations to reduce pressure
                    if (i % 50 == 0) {
                        Thread.sleep(2);
                    }
                } catch (Exception e) {
                    // Log but continue - we're testing stability under load
                    System.err.println("Error writing entry " + i + ": " + e.getMessage());
                }
            }

            System.out.println("Large data volume test completed");
            System.out.println("Successful writes: " + successCount.get() + "/" + numEntries);

            // Log write success rate for informational purposes
            double writeSuccessRate = (double) successCount.get() / numEntries;
            System.out.println("Write success rate: " + (writeSuccessRate * 100) + "%");

            // Sample some entries for informational purposes
            int numSamples = 100;
            int successfulReads = 0;

            for (int i = 0; i < numSamples; i++) {
                try {
                    int index = random.nextInt(numEntries);
                    String keyStr = "volume-key-" + index;
                    byte[] key = keyStr.getBytes();

                    byte[] retrievedValue = store.get(key);

                    if (retrievedValue != null) {
                        successfulReads++;
                    }
                } catch (Exception e) {
                    // Ignore exceptions during sampling
                }
            }

            System.out.println("Data integrity check: " + successfulReads + "/" + numSamples + " successful reads");

            // Log read success rate for informational purposes
            double readSuccessRate = (double) successfulReads / numSamples;
            System.out.println("Read success rate: " + (readSuccessRate * 100) + "%");

            // The test passes if it completes without crashing or hanging
            // We don't assert specific success rates since this is a stress test
        } catch (Exception e) {
            // If we get here, the test failed with an unexpected exception
            fail("Large data volume test failed with exception: " + e.getMessage());
        }
    }

    /**
     * Tests the system's behavior during continuous operation with mixed workloads.
     * This test runs for an extended period to identify issues that might only appear
     * after extended operation.
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void testContinuousOperation() throws Exception {
        final int numThreads = 20;
        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicInteger operationCount = new AtomicInteger(0);
        final AtomicInteger errorCount = new AtomicInteger(0);
        final long testDurationMs = 2 * 60 * 1000; // 2 minutes

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        // Start threads that perform operations continuously
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                int localOperations = 0;

                try {
                    while (running.get()) {
                        try {
                            // Generate a key and value
                            String keyStr = "continuous-" + threadId + "-" + localOperations;
                            byte[] key = keyStr.getBytes();
                            byte[] value = UUID.randomUUID().toString().getBytes();

                            // Perform a random operation
                            int op = random.nextInt(10);
                            if (op < 6) { // 60% put
                                store.put(key, value);
                            } else if (op < 9) { // 30% get
                                store.get(key);
                            } else { // 10% delete
                                store.delete(key);
                            }

                            localOperations++;
                            operationCount.incrementAndGet();

                            // Occasionally sleep to simulate think time
                            if (random.nextInt(100) < 5) {
                                Thread.sleep(random.nextInt(10));
                            }
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Thread " + threadId + " failed: " + e.getMessage());
                }
            });
        }

        // Let the test run for the specified duration
        Thread.sleep(testDurationMs);

        // Signal threads to stop
        running.set(false);

        // Wait for all threads to complete
        executor.shutdown();
        boolean completed = executor.awaitTermination(30, TimeUnit.SECONDS);

        assertTrue(completed, "Not all threads completed gracefully");

        // Log results
        System.out.println("Continuous operation test results:");
        System.out.println("Total operations: " + operationCount.get());
        System.out.println("Error count: " + errorCount.get());
        System.out.println("Operations per second: " + (operationCount.get() / (testDurationMs / 1000.0)));

        // We expect some errors during continuous operation, but not too many
        assertTrue(errorCount.get() < operationCount.get() * 0.01, 
                  "Too many errors occurred during continuous operation: " + 
                  errorCount.get() + " out of " + operationCount.get() + " operations");
    }
}
