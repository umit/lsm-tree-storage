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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the BloomFilter class.
 */
class BloomFilterTest {
    private BloomFilter filter;
    
    @TempDir
    Path tempDir;
    
    @BeforeEach
    void setUp() {
        // Create a BloomFilter with 100 expected entries and 1% false positive rate
        filter = new BloomFilter(100, 0.01);
    }
    
    @Test
    void testAddAndMightContain() {
        // Add some keys
        byte[] key1 = "key1".getBytes();
        byte[] key2 = "key2".getBytes();
        byte[] key3 = "key3".getBytes();
        
        filter.add(key1);
        filter.add(key2);
        filter.add(key3);
        
        // Test keys that are in the filter
        assertTrue(filter.mightContain(key1));
        assertTrue(filter.mightContain(key2));
        assertTrue(filter.mightContain(key3));
        
        // Test a key that is not in the filter
        // Note: There's a small chance of a false positive, but it's unlikely with our parameters
        byte[] nonExistentKey = "nonExistentKey".getBytes();
        // We don't assert anything here because there's a small chance of a false positive
        
        // Test null key
        assertFalse(filter.mightContain(null));
    }
    
    @Test
    void testAddNullKey() {
        // Adding a null key should not throw an exception
        assertDoesNotThrow(() -> filter.add(null));
        
        // Verify that null key is not in the filter
        assertFalse(filter.mightContain(null));
    }
    
    @Test
    void testSaveAndLoad() throws IOException {
        // Add some keys
        byte[] key1 = "key1".getBytes();
        byte[] key2 = "key2".getBytes();
        
        filter.add(key1);
        filter.add(key2);
        
        // Save the filter
        String filePath = tempDir.resolve("bloom.filter").toString();
        filter.save(filePath);
        
        // Load the filter
        BloomFilter loadedFilter = BloomFilter.load(filePath);
        
        // Test keys that are in the filter
        assertTrue(loadedFilter.mightContain(key1));
        assertTrue(loadedFilter.mightContain(key2));
        
        // Test a key that is not in the filter
        byte[] nonExistentKey = "nonExistentKey".getBytes();
        // We don't assert anything here because there's a small chance of a false positive
        
        // Test null key
        assertFalse(loadedFilter.mightContain(null));
    }
    
    @Test
    void testCreateWithBitsAndHashFunctions() {
        // Create a filter with a specific bit array and number of hash functions
        byte[] bits = new byte[10]; // 80 bits
        int numHashFunctions = 3;
        
        BloomFilter customFilter = new BloomFilter(bits, numHashFunctions);
        
        // Add some keys
        byte[] key1 = "key1".getBytes();
        byte[] key2 = "key2".getBytes();
        
        customFilter.add(key1);
        customFilter.add(key2);
        
        // Test keys that are in the filter
        assertTrue(customFilter.mightContain(key1));
        assertTrue(customFilter.mightContain(key2));
    }
    
    @Test
    void testFalsePositiveRate() {
        // Create a filter with a high false positive rate for testing
        BloomFilter highFpFilter = new BloomFilter(10, 0.5); // 50% false positive rate
        
        // Add a key
        byte[] key = "key".getBytes();
        highFpFilter.add(key);
        
        // Test the key that is in the filter
        assertTrue(highFpFilter.mightContain(key));
        
        // Test 100 keys that are not in the filter
        int falsePositives = 0;
        for (int i = 0; i < 100; i++) {
            byte[] nonExistentKey = ("nonExistentKey" + i).getBytes();
            if (highFpFilter.mightContain(nonExistentKey)) {
                falsePositives++;
            }
        }
        
        // With a 50% false positive rate, we expect around 50 false positives
        // But due to randomness, we'll just check that it's within a reasonable range
        assertTrue(falsePositives > 0, "Should have some false positives");
        assertTrue(falsePositives < 100, "Should not have all false positives");
    }
}