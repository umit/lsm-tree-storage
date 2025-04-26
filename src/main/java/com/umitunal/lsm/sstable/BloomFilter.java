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

package com.umitunal.lsm.sstable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Bloom filter implementation for efficient negative lookups.
 * A Bloom filter is a space-efficient probabilistic data structure that is used to test
 * whether an element is a member of a set. False positives are possible, but false negatives are not.
 * 
 * <p>This implementation uses Java 21's MemorySegment API for efficient off-heap memory management.</p>
 */
public class BloomFilter implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(BloomFilter.class);

    private final MemorySegment bitSegment;
    private final int numHashFunctions;
    private final Arena arena;
    private final int bitArraySize; // Size in bytes

    /**
     * Creates a new Bloom filter with the specified expected number of entries and false positive rate.
     * Uses a shared Arena for memory management.
     * 
     * @param expectedEntries the expected number of entries
     * @param falsePositiveRate the desired false positive rate (e.g., 0.01 for 1%)
     */
    public BloomFilter(int expectedEntries, double falsePositiveRate) {
        int numBits = optimalNumOfBits(expectedEntries, falsePositiveRate);
        this.bitArraySize = (numBits + 7) / 8; // Convert bits to bytes, rounding up
        this.numHashFunctions = optimalNumOfHashFunctions(expectedEntries, numBits);
        this.arena = Arena.ofShared();
        this.bitSegment = arena.allocate(bitArraySize);

        // Initialize the bit array to zeros
        bitSegment.fill((byte) 0);
    }

    /**
     * Creates a new Bloom filter with the specified bit array and number of hash functions.
     * Uses a shared Arena for memory management.
     * 
     * @param bits the bit array
     * @param numHashFunctions the number of hash functions
     */
    public BloomFilter(byte[] bits, int numHashFunctions) {
        this.bitArraySize = bits.length;
        this.numHashFunctions = numHashFunctions;
        this.arena = Arena.ofShared();
        this.bitSegment = arena.allocate(bitArraySize);

        // Copy the bits to the memory segment
        for (int i = 0; i < bits.length; i++) {
            bitSegment.set(ValueLayout.JAVA_BYTE, i, bits[i]);
        }
    }

    /**
     * Adds a key to the Bloom filter.
     * 
     * @param key the key to add
     */
    public void add(byte[] key) {
        if (key == null) {
            return;
        }

        // For each hash function, compute a hash and set the corresponding bit
        for (int i = 0; i < numHashFunctions; i++) {
            int hash = hash(key, i);
            int bitIndex = Math.abs(hash % (bitArraySize * 8));
            int byteIndex = bitIndex / 8;
            int bitOffset = bitIndex % 8;

            // Get the current byte value
            byte currentByte = bitSegment.get(ValueLayout.JAVA_BYTE, byteIndex);

            // Set the bit
            byte newByte = (byte) (currentByte | (1 << bitOffset));

            // Update the byte in the memory segment
            bitSegment.set(ValueLayout.JAVA_BYTE, byteIndex, newByte);
        }
    }

    /**
     * Checks if a key might be in the set.
     * 
     * @param key the key to check
     * @return true if the key might be in the set, false if it definitely isn't
     */
    public boolean mightContain(byte[] key) {
        if (key == null) {
            return false;
        }

        // For each hash function, compute a hash and check the corresponding bit
        for (int i = 0; i < numHashFunctions; i++) {
            int hash = hash(key, i);
            int bitIndex = Math.abs(hash % (bitArraySize * 8));
            int byteIndex = bitIndex / 8;
            int bitOffset = bitIndex % 8;

            // Get the byte from the memory segment
            byte currentByte = bitSegment.get(ValueLayout.JAVA_BYTE, byteIndex);

            // Check the bit
            if ((currentByte & (1 << bitOffset)) == 0) {
                return false; // Definitely not in the set
            }
        }

        return true; // Might be in the set
    }

    /**
     * Saves the Bloom filter to a file.
     * 
     * @param filePath the path to save the filter to
     * @throws IOException if an I/O error occurs
     */
    public void save(String filePath) throws IOException {
        try (FileChannel channel = FileChannel.open(
                Path.of(filePath),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            // Write the number of hash functions
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putInt(numHashFunctions);
            buffer.flip();
            channel.write(buffer);

            // Write the bit array
            byte[] bits = new byte[bitArraySize];
            for (int i = 0; i < bitArraySize; i++) {
                bits[i] = bitSegment.get(ValueLayout.JAVA_BYTE, i);
            }
            buffer = ByteBuffer.wrap(bits);
            channel.write(buffer);
        }
    }

    /**
     * Loads a Bloom filter from a file.
     * 
     * @param filePath the path to load the filter from
     * @return the loaded Bloom filter
     * @throws IOException if an I/O error occurs
     */
    public static BloomFilter load(String filePath) throws IOException {
        try (FileChannel channel = FileChannel.open(
                Path.of(filePath),
                StandardOpenOption.READ)) {

            // Read the number of hash functions
            ByteBuffer buffer = ByteBuffer.allocate(4);
            channel.read(buffer);
            buffer.flip();
            int numHashFunctions = buffer.getInt();

            // Read the bit array
            long fileSize = channel.size();
            int bitsSize = (int) (fileSize - 4); // Subtract the 4 bytes for numHashFunctions
            byte[] bits = new byte[bitsSize];
            buffer = ByteBuffer.wrap(bits);
            channel.read(buffer, 4); // Start reading after the numHashFunctions

            return new BloomFilter(bits, numHashFunctions);
        }
    }

    /**
     * Computes a hash of the key with the specified seed.
     * 
     * @param key the key to hash
     * @param seed the seed for the hash function
     * @return the hash value
     */
    private int hash(byte[] key, int seed) {
        int h = seed;
        for (byte b : key) {
            h = 31 * h + b;
        }
        return h;
    }

    /**
     * Calculates the optimal number of bits for a Bloom filter.
     * 
     * @param n the expected number of entries
     * @param p the desired false positive rate
     * @return the optimal number of bits
     */
    private int optimalNumOfBits(int n, double p) {
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    /**
     * Calculates the optimal number of hash functions for a Bloom filter.
     * 
     * @param n the expected number of entries
     * @param m the number of bits in the filter
     * @return the optimal number of hash functions
     */
    private int optimalNumOfHashFunctions(int n, int m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    /**
     * Closes this Bloom filter and releases any resources associated with it.
     * This method should be called when the Bloom filter is no longer needed.
     */
    @Override
    public void close() {
        try {
            if (arena != null) {
                arena.close();
            }
        } catch (IllegalStateException e) {
            // Arena is already closed, ignore
            logger.debug("Arena already closed", e);
        }
    }
}
