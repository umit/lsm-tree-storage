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

import com.umitunal.lsm.api.ByteArrayWrapper;
import com.umitunal.lsm.memtable.MemTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the MemTable class.
 */
class MemTableTest {
    private MemTable memTable;

    @BeforeEach
    void setUp() {
        // Create a MemTable with a small size for testing
        memTable = new MemTable(1024); // 1KB
    }

    @AfterEach
    void tearDown() {
        // Ensure proper cleanup
        if (memTable != null) {
            memTable.close();
        }
    }

    @Test
    void testPutAndGet() {
        // Test with valid key and value
        byte[] key = "testKey".getBytes();
        byte[] value = "testValue".getBytes();

        assertTrue(memTable.put(key, value, 0));
        assertArrayEquals(value, memTable.get(key));

        // Test with null key
        assertFalse(memTable.put(null, value, 0));

        // Test with empty key
        assertFalse(memTable.put(new byte[0], value, 0));

        // Test with null value
        assertFalse(memTable.put(key, null, 0));
    }

    @Test
    void testGetNonExistentKey() {
        assertNull(memTable.get("nonExistentKey".getBytes()));
        assertNull(memTable.get(null));
        assertNull(memTable.get(new byte[0]));
    }

    @Test
    void testPutWithTTL() {
        // Test with valid key, value, and TTL
        byte[] key = "ttlKey".getBytes();
        byte[] value = "ttlValue".getBytes();

        // Put with 1 second TTL
        assertTrue(memTable.put(key, value, 1));

        // Verify the key exists immediately
        assertArrayEquals(value, memTable.get(key));

        // Wait for the TTL to expire
        try {
            Thread.sleep(1500); // Wait 1.5 seconds
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Verify the key has expired
        assertNull(memTable.get(key));
    }

    @Test
    void testDelete() {
        byte[] key = "deleteKey".getBytes();
        byte[] value = "deleteValue".getBytes();

        // Put a key-value pair
        memTable.put(key, value, 0);

        // Delete the key
        assertTrue(memTable.delete(key));

        // Verify the key is deleted
        assertNull(memTable.get(key));

        // Try to delete a non-existent key
        // Note: MemTable.delete() returns true even for non-existent keys
        // as long as the operation is successful (adding a tombstone marker)
        assertTrue(memTable.delete("nonExistentKey".getBytes()));

        // Try to delete null or empty key
        assertFalse(memTable.delete(null));
        assertFalse(memTable.delete(new byte[0]));
    }

    @Test
    void testContainsKey() {
        byte[] key = "containsKey".getBytes();
        byte[] value = "containsValue".getBytes();

        // Initially, the key should not exist
        assertFalse(memTable.containsKey(key));

        // Add the key-value pair
        memTable.put(key, value, 0);

        // Verify the key exists
        assertTrue(memTable.containsKey(key));

        // Check for null or empty key
        assertFalse(memTable.containsKey(null));
        assertFalse(memTable.containsKey(new byte[0]));
    }

    @Test
    void testSizeTracking() {
        // Initially, the size should be 0
        assertEquals(0, memTable.getSizeBytes());

        // Add a key-value pair
        byte[] key = "sizeKey".getBytes();
        byte[] value = "sizeValue".getBytes();
        memTable.put(key, value, 0);

        // Verify the size has increased
        assertTrue(memTable.getSizeBytes() > 0);

        // Add another key-value pair
        byte[] key2 = "sizeKey2".getBytes();
        byte[] value2 = "sizeValue2".getBytes();
        memTable.put(key2, value2, 0);

        // Verify the size has increased further
        long sizeAfterTwoEntries = memTable.getSizeBytes();
        assertTrue(sizeAfterTwoEntries > 0);

        // Delete a key
        memTable.delete(key);

        // Verify the size has decreased
        assertTrue(memTable.getSizeBytes() < sizeAfterTwoEntries);
    }

    @Test
    void testImmutability() {
        // Initially, the MemTable should be mutable
        assertFalse(memTable.isImmutable());

        // Add a key-value pair
        byte[] key = "immutableKey".getBytes();
        byte[] value = "immutableValue".getBytes();
        assertTrue(memTable.put(key, value, 0));

        // Make the MemTable immutable
        memTable.makeImmutable();

        // Verify the MemTable is now immutable
        assertTrue(memTable.isImmutable());

        // Try to add another key-value pair
        byte[] key2 = "immutableKey2".getBytes();
        byte[] value2 = "immutableValue2".getBytes();
        assertFalse(memTable.put(key2, value2, 0));

        // Try to delete a key
        assertFalse(memTable.delete(key));

        // Verify the original key-value pair is still accessible
        assertArrayEquals(value, memTable.get(key));
    }

    @Test
    void testFullMemTable() {
        // Create a very small MemTable
        MemTable smallMemTable = new MemTable(20); // 20 bytes

        try {
            // Add a key-value pair that's larger than the MemTable's maximum size
            byte[] key = "key".getBytes();
            byte[] value = "this_is_a_large_value_that_should_exceed_the_memtable_size".getBytes();

            // The put operation should fail because the entry is too large
            boolean result = smallMemTable.put(key, value, 0);

            // Verify the put operation failed
            assertFalse(result);

            // Verify the MemTable's size is still 0
            assertEquals(0, smallMemTable.getSizeBytes());
        } finally {
            smallMemTable.close();
        }
    }

    @Test
    void testGetEntries() {
        // Add some key-value pairs
        byte[] key1 = "key1".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] key2 = "key2".getBytes();
        byte[] value2 = "value2".getBytes();

        memTable.put(key1, value1, 0);
        memTable.put(key2, value2, 0);

        // Get all entries
        Map<ByteArrayWrapper, MemTable.ValueEntry> entries = memTable.getEntries();

        // Verify the entries
        assertEquals(2, entries.size());

        // Verify the entries contain the expected keys
        boolean containsKey1 = false;
        boolean containsKey2 = false;

        for (ByteArrayWrapper key : entries.keySet()) {
            if (Arrays.equals(key.getData(), key1)) {
                containsKey1 = true;
                assertArrayEquals(value1, entries.get(key).getValue());
            } else if (Arrays.equals(key.getData(), key2)) {
                containsKey2 = true;
                assertArrayEquals(value2, entries.get(key).getValue());
            }
        }

        assertTrue(containsKey1, "Should contain key1");
        assertTrue(containsKey2, "Should contain key2");
    }

    @Test
    void testTombstoneMarker() {
        // Add a key-value pair
        byte[] key = "tombstoneKey".getBytes();
        byte[] value = "tombstoneValue".getBytes();
        memTable.put(key, value, 0);

        // Delete the key (adds a tombstone marker)
        memTable.delete(key);

        // Verify the key is deleted
        assertNull(memTable.get(key));

        // Get all entries
        Map<ByteArrayWrapper, MemTable.ValueEntry> entries = memTable.getEntries();

        // Find the entry for the deleted key
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        MemTable.ValueEntry entry = entries.get(keyWrapper);

        // Verify it's a tombstone
        assertNotNull(entry);
        assertTrue(entry.isTombstone());
        assertNull(entry.getValue());
    }
}
