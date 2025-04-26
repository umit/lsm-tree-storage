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

package com.umitunal.lsm.benchmark;

import com.umitunal.lsm.api.KeyValueIterator;
import com.umitunal.lsm.core.store.LSMStore;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH Benchmarks for LSMStore operations.
 * 
 * To run the benchmark:
 * mvn clean package
 * java -jar target/benchmarks.jar
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms2G", "-Xmx2G", "--enable-preview"})
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class LSMStoreBenchmark {

    private LSMStore store;
    private Path tempDir;
    private byte[][] keys;
    private byte[][] values;
    private Random random;

    private static final int DATA_SIZE = 10_000;
    private static final int KEY_SIZE = 16;
    private static final int VALUE_SIZE = 100;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        // Create a temporary directory for the LSMStore
        tempDir = Files.createTempDirectory("lsm-benchmark");

        // Initialize the LSMStore with 1MB MemTable size and compaction threshold of 4
        store = new LSMStore(1024 * 1024, tempDir.toString(), 4);

        // Initialize random number generator
        random = new Random(42); // Fixed seed for reproducibility

        // Generate random keys and values
        keys = new byte[DATA_SIZE][];
        values = new byte[DATA_SIZE][];

        for (int i = 0; i < DATA_SIZE; i++) {
            keys[i] = generateRandomBytes(KEY_SIZE);
            values[i] = generateRandomBytes(VALUE_SIZE);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        // Shutdown the store
        if (store != null) {
            store.shutdown();
        }

        // Clean up the temporary directory
        try {
            Files.walk(tempDir)
                .sorted((a, b) -> -a.compareTo(b)) // Reverse order to delete files before directories
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        System.err.println("Failed to delete: " + path);
                    }
                });
        } catch (IOException e) {
            System.err.println("Failed to clean up temporary directory: " + e.getMessage());
        }
    }

    /**
     * Benchmark for the put operation.
     */
    @Benchmark
    public void benchmarkPut(Blackhole blackhole) {
        int index = random.nextInt(DATA_SIZE);
        boolean result = store.put(keys[index], values[index]);
        blackhole.consume(result);
    }

    /**
     * Benchmark for the get operation.
     */
    @Benchmark
    public void benchmarkGet(Blackhole blackhole) {
        int index = random.nextInt(DATA_SIZE);
        byte[] result = store.get(keys[index]);
        blackhole.consume(result);
    }

    /**
     * Benchmark for the delete operation.
     */
    @Benchmark
    public void benchmarkDelete(Blackhole blackhole) {
        int index = random.nextInt(DATA_SIZE);
        boolean result = store.delete(keys[index]);
        blackhole.consume(result);
    }

    /**
     * Benchmark for the getRange operation.
     */
    @Benchmark
    public void benchmarkGetRange(Blackhole blackhole) {
        int startIndex = random.nextInt(DATA_SIZE - 100);
        int endIndex = startIndex + 100;

        Map<byte[], byte[]> result = store.getRange(keys[startIndex], keys[endIndex]);
        blackhole.consume(result);
    }

    /**
     * Benchmark for the getIterator operation.
     */
    @Benchmark
    public void benchmarkGetIterator(Blackhole blackhole) {
        int startIndex = random.nextInt(DATA_SIZE - 100);
        int endIndex = startIndex + 100;

        try (KeyValueIterator iterator = store.getIterator(keys[startIndex], keys[endIndex])) {
            while (iterator.hasNext()) {
                Map.Entry<byte[], byte[]> entry = iterator.next();
                blackhole.consume(entry);
            }
        }
    }

    /**
     * Benchmark for sequential put operations.
     */
    @Benchmark
    public void benchmarkSequentialPut(Blackhole blackhole) {
        for (int i = 0; i < 100; i++) {
            byte[] key = generateRandomBytes(KEY_SIZE);
            byte[] value = generateRandomBytes(VALUE_SIZE);
            boolean result = store.put(key, value);
            blackhole.consume(result);
        }
    }

    /**
     * Benchmark for put operations with TTL.
     */
    @Benchmark
    public void benchmarkPutWithTTL(Blackhole blackhole) {
        int index = random.nextInt(DATA_SIZE);
        boolean result = store.put(keys[index], values[index], 60); // 60 seconds TTL
        blackhole.consume(result);
    }

    /**
     * Benchmark for the containsKey operation.
     */
    @Benchmark
    public void benchmarkContainsKey(Blackhole blackhole) {
        int index = random.nextInt(DATA_SIZE);
        boolean result = store.containsKey(keys[index]);
        blackhole.consume(result);
    }

    /**
     * Benchmark for the listKeys operation.
     */
    @Benchmark
    public void benchmarkListKeys(Blackhole blackhole) {
        List<byte[]> result = store.listKeys();
        blackhole.consume(result);
    }

    /**
     * Benchmark for the size operation.
     */
    @Benchmark
    public void benchmarkSize(Blackhole blackhole) {
        int result = store.size();
        blackhole.consume(result);
    }

    /**
     * Benchmark for the clear operation.
     * Note: This is a destructive operation that removes all data.
     * It's included here for completeness but should be used with caution.
     */
    @Benchmark
    @Fork(value = 1, jvmArgs = {"--enable-preview"}) // Use a separate fork to avoid affecting other benchmarks
    public void benchmarkClear(Blackhole blackhole) {
        // Add some data first to ensure there's something to clear
        for (int i = 0; i < 10; i++) {
            byte[] key = generateRandomBytes(KEY_SIZE);
            byte[] value = generateRandomBytes(VALUE_SIZE);
            store.put(key, value);
        }

        // Clear the store
        store.clear();

        // Consume the result of size to verify the clear worked
        int size = store.size();
        blackhole.consume(size);
    }

    /**
     * Generate random bytes of specified size.
     */
    private byte[] generateRandomBytes(int size) {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    /**
     * Benchmark for e-commerce read-heavy workload.
     * In e-commerce applications, reads (product browsing) are much more frequent than writes.
     * This benchmark simulates a 90% read, 10% write workload.
     */
    @Benchmark
    public void benchmarkECommerceReadHeavyWorkload(Blackhole blackhole) {
        // 90% reads, 10% writes
        boolean isRead = random.nextInt(100) < 90;

        if (isRead) {
            // Read operation - simulate product browsing
            int index = random.nextInt(DATA_SIZE);
            byte[] result = store.get(keys[index]);
            blackhole.consume(result);

            // Occasionally perform a range query (related products)
            if (random.nextInt(10) == 0) {
                int startIndex = Math.max(0, index - 5);
                int endIndex = Math.min(DATA_SIZE - 1, index + 5);

                Map<byte[], byte[]> relatedProducts = store.getRange(keys[startIndex], keys[endIndex]);
                blackhole.consume(relatedProducts);
            }
        } else {
            // Write operation - simulate purchase or cart update
            int index = random.nextInt(DATA_SIZE);
            byte[] key = generateRandomBytes(KEY_SIZE);
            byte[] value = generateRandomBytes(VALUE_SIZE);
            boolean result = store.put(key, value);
            blackhole.consume(result);
        }
    }

    /**
     * Benchmark for burst traffic patterns.
     * This simulates traffic spikes that occur during flash sales or promotional events.
     * It performs a burst of operations in quick succession.
     */
    @Benchmark
    public void benchmarkBurstTraffic(Blackhole blackhole) {
        // Perform a burst of 20 operations in quick succession
        for (int i = 0; i < 20; i++) {
            int index = random.nextInt(DATA_SIZE);

            // 80% reads, 20% writes during burst
            boolean isRead = random.nextInt(100) < 80;

            if (isRead) {
                byte[] result = store.get(keys[index]);
                blackhole.consume(result);
            } else {
                byte[] key = generateRandomBytes(KEY_SIZE);
                byte[] value = generateRandomBytes(VALUE_SIZE);
                boolean result = store.put(key, value);
                blackhole.consume(result);
            }
        }
    }

    /**
     * Benchmark for high contention scenario.
     * This simulates situations where many operations target the same small set of keys,
     * such as popular products or flash sale items.
     */
    @Benchmark
    public void benchmarkHighContention(Blackhole blackhole) {
        // Use a small set of "hot" keys (10% of total)
        int hotKeyCount = DATA_SIZE / 10;
        int hotKeyIndex = random.nextInt(hotKeyCount);

        // 80% reads, 20% writes
        boolean isRead = random.nextInt(100) < 80;

        if (isRead) {
            byte[] result = store.get(keys[hotKeyIndex]);
            blackhole.consume(result);
        } else {
            boolean result = store.put(keys[hotKeyIndex], generateRandomBytes(VALUE_SIZE));
            blackhole.consume(result);
        }
    }

    /**
     * Benchmark for mixed operations that simulates a realistic e-commerce workload.
     * This combines various operations in proportions typical of e-commerce applications:
     * - Product browsing (get)
     * - Related product viewing (range queries)
     * - Adding to cart (put)
     * - Removing from cart (delete)
     */
    @Benchmark
    public void benchmarkECommerceWorkload(Blackhole blackhole) {
        int operationType = random.nextInt(100);

        if (operationType < 70) {
            // 70% - Product browsing (get)
            int index = random.nextInt(DATA_SIZE);
            byte[] result = store.get(keys[index]);
            blackhole.consume(result);
        } else if (operationType < 85) {
            // 15% - Related product viewing (range query)
            int startIndex = random.nextInt(DATA_SIZE - 10);
            int endIndex = startIndex + 10;
            Map<byte[], byte[]> result = store.getRange(keys[startIndex], keys[endIndex]);
            blackhole.consume(result);
        } else if (operationType < 95) {
            // 10% - Adding to cart (put)
            int index = random.nextInt(DATA_SIZE);
            boolean result = store.put(generateRandomBytes(KEY_SIZE), generateRandomBytes(VALUE_SIZE));
            blackhole.consume(result);
        } else {
            // 5% - Removing from cart (delete)
            int index = random.nextInt(DATA_SIZE);
            boolean result = store.delete(keys[index]);
            blackhole.consume(result);
        }
    }

    /**
     * Main method to run the benchmark from IDE.
     */
    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
            .include(LSMStoreBenchmark.class.getSimpleName())
            .jvmArgs("--enable-preview") // Required for Java 21 preview features
            .build();
        new Runner(options).run();
    }
}
