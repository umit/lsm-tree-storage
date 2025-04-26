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

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the SSTableIterator interface and its implementations.
 */
class SSTableIteratorTest {
    
    @Test
    void testInMemoryIterator() {
        // Create some entries
        byte[] key1 = "key1".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] key2 = "key2".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] key3 = "key3".getBytes();
        byte[] value3 = "value3".getBytes();
        
        long timestamp = System.currentTimeMillis();
        
        SSTableEntry entry1 = SSTableEntry.of(key1, value1, timestamp);
        SSTableEntry entry2 = SSTableEntry.of(key2, value2, timestamp);
        SSTableEntry entry3 = SSTableEntry.of(key3, value3, timestamp);
        
        // Create an array of entries
        SSTableEntry[] entries = {entry1, entry2, entry3};
        
        // Create an InMemoryIterator
        SSTableIterator.InMemoryIterator iterator = new SSTableIterator.InMemoryIterator(entries);
        
        // Test hasNext()
        assertTrue(iterator.hasNext());
        
        // Test next()
        Map.Entry<byte[], byte[]> nextEntry = iterator.next();
        assertArrayEquals(key1, nextEntry.getKey());
        assertArrayEquals(value1, nextEntry.getValue());
        
        // Test currentEntry()
        SSTableEntry currentEntry = iterator.currentEntry();
        assertSame(entry1, currentEntry);
        
        // Test peekNextKey()
        assertArrayEquals(key2, iterator.peekNextKey());
        
        // Test next() again
        nextEntry = iterator.next();
        assertArrayEquals(key2, nextEntry.getKey());
        assertArrayEquals(value2, nextEntry.getValue());
        
        // Test currentEntry() again
        currentEntry = iterator.currentEntry();
        assertSame(entry2, currentEntry);
        
        // Test next() one more time
        nextEntry = iterator.next();
        assertArrayEquals(key3, nextEntry.getKey());
        assertArrayEquals(value3, nextEntry.getValue());
        
        // Test hasNext() at the end
        assertFalse(iterator.hasNext());
        
        // Test peekNextKey() at the end
        assertNull(iterator.peekNextKey());
        
        // Test next() at the end
        assertThrows(NoSuchElementException.class, iterator::next);
        
        // Test close()
        assertDoesNotThrow(iterator::close);
    }
    
    @Test
    void testInMemoryIteratorWithEmptyArray() {
        // Create an InMemoryIterator with an empty array
        SSTableIterator.InMemoryIterator iterator = new SSTableIterator.InMemoryIterator(new SSTableEntry[0]);
        
        // Test hasNext()
        assertFalse(iterator.hasNext());
        
        // Test peekNextKey()
        assertNull(iterator.peekNextKey());
        
        // Test next()
        assertThrows(NoSuchElementException.class, iterator::next);
        
        // Test currentEntry()
        assertThrows(NoSuchElementException.class, iterator::currentEntry);
        
        // Test close()
        assertDoesNotThrow(iterator::close);
    }
    
    @Test
    void testInMemoryIteratorWithNullArray() {
        // Create an InMemoryIterator with a null array
        assertThrows(NullPointerException.class, () -> new SSTableIterator.InMemoryIterator(null));
    }
    
    @Test
    void testInMemoryIteratorWithTombstones() {
        // Create some entries including a tombstone
        byte[] key1 = "key1".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] key2 = "key2".getBytes();
        byte[] key3 = "key3".getBytes();
        byte[] value3 = "value3".getBytes();
        
        long timestamp = System.currentTimeMillis();
        
        SSTableEntry entry1 = SSTableEntry.of(key1, value1, timestamp);
        SSTableEntry entry2 = SSTableEntry.tombstone(key2, timestamp); // Tombstone
        SSTableEntry entry3 = SSTableEntry.of(key3, value3, timestamp);
        
        // Create an array of entries
        SSTableEntry[] entries = {entry1, entry2, entry3};
        
        // Create an InMemoryIterator
        SSTableIterator.InMemoryIterator iterator = new SSTableIterator.InMemoryIterator(entries);
        
        // Test iteration
        assertTrue(iterator.hasNext());
        Map.Entry<byte[], byte[]> nextEntry = iterator.next();
        assertArrayEquals(key1, nextEntry.getKey());
        assertArrayEquals(value1, nextEntry.getValue());
        
        assertTrue(iterator.hasNext());
        nextEntry = iterator.next();
        assertArrayEquals(key2, nextEntry.getKey());
        assertNull(nextEntry.getValue()); // Value is null for tombstone
        
        assertTrue(iterator.hasNext());
        nextEntry = iterator.next();
        assertArrayEquals(key3, nextEntry.getKey());
        assertArrayEquals(value3, nextEntry.getValue());
        
        assertFalse(iterator.hasNext());
    }
}