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

import com.umitunal.lsm.api.KeyValueIterator;
import com.umitunal.lsm.core.store.LSMStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the LSMStore class.
 */
class LSMStoreTest {
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

    @Test
    void testPutAndGet() {
        // Test with valid key and value
        byte[] key = "testKey".getBytes();
        byte[] value = "testValue".getBytes();

        assertTrue(store.put(key, value));
        assertArrayEquals(value, store.get(key));

        // Test with null key
        assertFalse(store.put(null, value));

        // Test with empty key
        assertFalse(store.put(new byte[0], value));

        // Test with null value
        assertFalse(store.put(key, null));
    }

    @Test
    void testGetNonExistentKey() {
        assertNull(store.get("nonExistentKey".getBytes()));
        assertNull(store.get(null));
        assertNull(store.get(new byte[0]));
    }

    @Test
    void testPutWithTTL() {
        // Test with valid key, value, and TTL
        byte[] key = "ttlKey".getBytes();
        byte[] value = "ttlValue".getBytes();

        // Put with 1 second TTL
        assertTrue(store.put(key, value, 1));

        // Verify the key exists immediately
        assertArrayEquals(value, store.get(key));

        // Wait for the TTL to expire
        try {
            Thread.sleep(1500); // Wait 1.5 seconds
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Verify the key has expired
        assertNull(store.get(key));
    }

    @Test
    void testDelete() {
        byte[] key = "deleteKey".getBytes();
        byte[] value = "deleteValue".getBytes();

        // Put a key-value pair
        store.put(key, value);

        // Delete the key
        assertTrue(store.delete(key));

        // Verify the key is deleted
        assertNull(store.get(key));

        // Try to delete a non-existent key
        assertFalse(store.delete("nonExistentKey".getBytes()));

        // Try to delete null or empty key
        assertFalse(store.delete(null));
        assertFalse(store.delete(new byte[0]));
    }

    @Test
    void testContainsKey() {
        byte[] key = "containsKey".getBytes();
        byte[] value = "containsValue".getBytes();

        // Initially, the key should not exist
        assertFalse(store.containsKey(key));

        // Add the key-value pair
        store.put(key, value);

        // Verify the key exists
        assertTrue(store.containsKey(key));

        // Check for null or empty key
        assertFalse(store.containsKey(null));
        assertFalse(store.containsKey(new byte[0]));
    }

    @Test
    void testSize() {
        // Initially, the store should be empty
        assertEquals(0, store.size());

        // Add some key-value pairs
        store.put("key1".getBytes(), "value1".getBytes());
        assertEquals(1, store.size());

        store.put("key2".getBytes(), "value2".getBytes());
        assertEquals(2, store.size());

        // Delete a key
        store.delete("key1".getBytes());
        assertEquals(1, store.size());
    }

    @Test
    void testBasicOperationsWithPersistence() throws IOException {
        // Create a store with a custom data directory
        Path dataDir = tempDir.resolve("persistence_test");
        LSMStore testStore = null;

        try {
            // Create the initial store
            testStore = new LSMStore(1024 * 1024, dataDir.toString(), 4);

            // Add some key-value pairs
            byte[] key1 = "test1".getBytes();
            byte[] value1 = "value1".getBytes();
            byte[] key2 = "test2".getBytes();
            byte[] value2 = "value2".getBytes();

            testStore.put(key1, value1);
            testStore.put(key2, value2);

            // Verify the keys exist
            assertArrayEquals(value1, testStore.get(key1));
            assertArrayEquals(value2, testStore.get(key2));

            // Add a new key and delete an existing key
            byte[] key3 = "test3".getBytes();
            byte[] value3 = "value3".getBytes();

            testStore.put(key3, value3);
            testStore.delete(key1);

            // Verify the changes
            assertNull(testStore.get(key1));
            assertArrayEquals(value2, testStore.get(key2));
            assertArrayEquals(value3, testStore.get(key3));

            // Test range query
            Map<byte[], byte[]> range = testStore.getRange(key2, null);
            assertEquals(2, range.size());
            assertArrayEquals(value2, range.get(key2));
            assertArrayEquals(value3, range.get(key3));

            // Test iterator
            try (KeyValueIterator iterator = testStore.getIterator(null, null)) {
                assertTrue(iterator.hasNext());
                Map.Entry<byte[], byte[]> entry = iterator.next();
                assertArrayEquals(key2, entry.getKey());
                assertArrayEquals(value2, entry.getValue());

                assertTrue(iterator.hasNext());
                entry = iterator.next();
                assertArrayEquals(key3, entry.getKey());
                assertArrayEquals(value3, entry.getValue());

                assertFalse(iterator.hasNext());
            }
        } finally {
            // Ensure proper cleanup
            if (testStore != null) {
                testStore.shutdown();
            }
        }
    }

    @Test
    void testClear() {
        // Add some key-value pairs
        store.put("key1".getBytes(), "value1".getBytes());
        store.put("key2".getBytes(), "value2".getBytes());

        // Verify the store is not empty
        assertEquals(2, store.size());

        // Clear the store
        store.clear();

        // Verify the store is empty
        assertEquals(0, store.size());
        assertTrue(store.listKeys().isEmpty());
    }

    @Test
    void testListKeys() {
        // Initially, the store should be empty
        assertTrue(store.listKeys().isEmpty());

        // Add some key-value pairs
        byte[] key1 = "key1".getBytes();
        byte[] key2 = "key2".getBytes();
        byte[] key3 = "key3".getBytes();

        store.put(key1, "value1".getBytes());
        store.put(key2, "value2".getBytes());
        store.put(key3, "value3".getBytes());

        // Get the list of keys
        List<byte[]> keys = store.listKeys();

        // Verify the list contains all the keys
        assertEquals(3, keys.size());

        // Helper method to check if a byte array is in the list
        boolean containsKey1 = false;
        boolean containsKey2 = false;
        boolean containsKey3 = false;

        for (byte[] key : keys) {
            if (java.util.Arrays.equals(key, key1)) {
                containsKey1 = true;
            } else if (java.util.Arrays.equals(key, key2)) {
                containsKey2 = true;
            } else if (java.util.Arrays.equals(key, key3)) {
                containsKey3 = true;
            }
        }

        assertTrue(containsKey1, "Should contain key1");
        assertTrue(containsKey2, "Should contain key2");
        assertTrue(containsKey3, "Should contain key3");
    }

    @Test
    void testMemTableSwitching() {
        // Create a store with a very small MemTable size to force switching
        // Use 1KB instead of 100 bytes to ensure at least one entry can fit
        LSMStore smallStore = new LSMStore(1024, tempDir.toString(), 4);

        try {
            // Add enough key-value pairs to fill the MemTable
            for (int i = 0; i < 10; i++) {
                byte[] key = ("key" + i).getBytes();
                byte[] value = ("value" + i).getBytes();
                assertTrue(smallStore.put(key, value));
            }

            // Verify we can still retrieve all values
            for (int i = 0; i < 10; i++) {
                byte[] key = ("key" + i).getBytes();
                byte[] expectedValue = ("value" + i).getBytes();
                assertArrayEquals(expectedValue, smallStore.get(key));
            }
        } finally {
            smallStore.shutdown();
        }
    }

    @Test
    void testGetRange() {
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

        // Test full range (null to null)
        Map<byte[], byte[]> fullRange = store.getRange(null, null);
        assertEquals(5, fullRange.size());
        assertArrayEquals(value1, fullRange.get(key1));
        assertArrayEquals(value2, fullRange.get(key2));
        assertArrayEquals(value3, fullRange.get(key3));
        assertArrayEquals(value4, fullRange.get(key4));
        assertArrayEquals(value5, fullRange.get(key5));

        // Test partial range (key2 to key4)
        Map<byte[], byte[]> partialRange = store.getRange(key2, key5);
        assertEquals(3, partialRange.size());
        assertArrayEquals(value2, partialRange.get(key2));
        assertArrayEquals(value3, partialRange.get(key3));
        assertArrayEquals(value4, partialRange.get(key4));

        // Test range with start only (key3 to null)
        Map<byte[], byte[]> startRange = store.getRange(key3, null);
        assertEquals(3, startRange.size());
        assertArrayEquals(value3, startRange.get(key3));
        assertArrayEquals(value4, startRange.get(key4));
        assertArrayEquals(value5, startRange.get(key5));

        // Test range with end only (null to key3)
        Map<byte[], byte[]> endRange = store.getRange(null, key3);
        assertEquals(2, endRange.size());
        assertArrayEquals(value1, endRange.get(key1));
        assertArrayEquals(value2, endRange.get(key2));

        // Test empty range (key5 to key1)
        Map<byte[], byte[]> emptyRange = store.getRange(key5, key1);
        assertEquals(0, emptyRange.size());
    }

    @Test
    void testGetIterator() {
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

        // Test full range iterator
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
        } catch (Exception e) {
            fail("Iterator should not throw exception: " + e.getMessage());
        }

        // Test partial range iterator
        try (KeyValueIterator iterator = store.getIterator(key2, null)) {
            assertTrue(iterator.hasNext());
            Map.Entry<byte[], byte[]> entry1 = iterator.next();
            assertArrayEquals(key2, entry1.getKey());
            assertArrayEquals(value2, entry1.getValue());

            assertTrue(iterator.hasNext());
            Map.Entry<byte[], byte[]> entry2 = iterator.next();
            assertArrayEquals(key3, entry2.getKey());
            assertArrayEquals(value3, entry2.getValue());

            assertFalse(iterator.hasNext());
        } catch (Exception e) {
            fail("Iterator should not throw exception: " + e.getMessage());
        }

        // Test peek functionality
        try (KeyValueIterator iterator = store.getIterator(null, null)) {
            assertArrayEquals(key1, iterator.peekNextKey());
            iterator.next(); // Consume key1

            assertArrayEquals(key2, iterator.peekNextKey());
            iterator.next(); // Consume key2

            assertArrayEquals(key3, iterator.peekNextKey());
            iterator.next(); // Consume key3

            assertNull(iterator.peekNextKey());
        } catch (Exception e) {
            fail("Iterator should not throw exception: " + e.getMessage());
        }
    }
}
