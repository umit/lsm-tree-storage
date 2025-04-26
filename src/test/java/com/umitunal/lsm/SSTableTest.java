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

import com.umitunal.lsm.memtable.MemTable;
import com.umitunal.lsm.sstable.SSTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the SSTable class.
 */
class SSTableTest {
    private SSTable ssTable;
    private MemTable memTable;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        // Create a MemTable with test data
        memTable = new MemTable(1024); // 1KB
        
        // Add some key-value pairs
        memTable.put("key1".getBytes(), "value1".getBytes(), 0);
        memTable.put("key2".getBytes(), "value2".getBytes(), 0);
        memTable.put("key3".getBytes(), "value3".getBytes(), 0);
        
        // Add a tombstone
        memTable.put("key4".getBytes(), "value4".getBytes(), 0);
        memTable.delete("key4".getBytes());
    }

    @AfterEach
    void tearDown() {
        // Ensure proper cleanup
        if (ssTable != null) {
            ssTable.close();
        }
        if (memTable != null) {
            memTable.close();
        }
    }

    @Test
    void testCreateFromMemTable() throws IOException {
        // Create an SSTable from the MemTable
        ssTable = new SSTable(memTable, tempDir.toString(), 0, 1);
        
        // Verify the SSTable was created successfully
        assertNotNull(ssTable);
        assertEquals(0, ssTable.getLevel());
        assertEquals(1, ssTable.getSequenceNumber());
    }

    @Test
    void testGetExistingKey() throws IOException {
        // Create an SSTable from the MemTable
        ssTable = new SSTable(memTable, tempDir.toString(), 0, 1);
        
        // Test getting existing keys
        byte[] value1 = ssTable.get("key1".getBytes());
        assertNotNull(value1);
        assertArrayEquals("value1".getBytes(), value1);
        
        byte[] value2 = ssTable.get("key2".getBytes());
        assertNotNull(value2);
        assertArrayEquals("value2".getBytes(), value2);
        
        byte[] value3 = ssTable.get("key3".getBytes());
        assertNotNull(value3);
        assertArrayEquals("value3".getBytes(), value3);
    }

    @Test
    void testGetNonExistentKey() throws IOException {
        // Create an SSTable from the MemTable
        ssTable = new SSTable(memTable, tempDir.toString(), 0, 1);
        
        // Test getting a non-existent key
        byte[] value = ssTable.get("nonExistentKey".getBytes());
        assertNull(value);
    }

    @Test
    void testGetDeletedKey() throws IOException {
        // Create an SSTable from the MemTable
        ssTable = new SSTable(memTable, tempDir.toString(), 0, 1);
        
        // Test getting a deleted key
        byte[] value = ssTable.get("key4".getBytes());
        assertNull(value);
    }

    @Test
    void testMightContain() throws IOException {
        // Create an SSTable from the MemTable
        ssTable = new SSTable(memTable, tempDir.toString(), 0, 1);
        
        // Test might contain for existing keys
        assertTrue(ssTable.mightContain("key1".getBytes()));
        assertTrue(ssTable.mightContain("key2".getBytes()));
        assertTrue(ssTable.mightContain("key3".getBytes()));
        
        // Test might contain for deleted key
        assertTrue(ssTable.mightContain("key4".getBytes()));
        
        // Test might contain for non-existent key
        // Note: This might return true due to false positives in the bloom filter
        // So we don't assert anything here
    }

    @Test
    void testGetRange() throws IOException {
        // Create an SSTable from the MemTable
        ssTable = new SSTable(memTable, tempDir.toString(), 0, 1);
        
        // Test getting a range of keys
        Map<byte[], byte[]> range = ssTable.getRange("key1".getBytes(), "key3".getBytes());
        
        // Verify the range contains the expected keys
        assertEquals(2, range.size());
        
        boolean containsKey1 = false;
        boolean containsKey2 = false;
        
        for (Map.Entry<byte[], byte[]> entry : range.entrySet()) {
            if (Arrays.equals(entry.getKey(), "key1".getBytes())) {
                containsKey1 = true;
                assertArrayEquals("value1".getBytes(), entry.getValue());
            } else if (Arrays.equals(entry.getKey(), "key2".getBytes())) {
                containsKey2 = true;
                assertArrayEquals("value2".getBytes(), entry.getValue());
            }
        }
        
        assertTrue(containsKey1, "Range should contain key1");
        assertTrue(containsKey2, "Range should contain key2");
    }

    @Test
    void testListKeys() throws IOException {
        // Create an SSTable from the MemTable
        ssTable = new SSTable(memTable, tempDir.toString(), 0, 1);
        
        // Test listing all keys
        var keys = ssTable.listKeys();
        
        // Verify the list contains the expected keys
        assertEquals(3, keys.size()); // 3 keys (key4 is deleted)
        
        boolean containsKey1 = false;
        boolean containsKey2 = false;
        boolean containsKey3 = false;
        
        for (byte[] key : keys) {
            if (Arrays.equals(key, "key1".getBytes())) {
                containsKey1 = true;
            } else if (Arrays.equals(key, "key2".getBytes())) {
                containsKey2 = true;
            } else if (Arrays.equals(key, "key3".getBytes())) {
                containsKey3 = true;
            }
        }
        
        assertTrue(containsKey1, "Should contain key1");
        assertTrue(containsKey2, "Should contain key2");
        assertTrue(containsKey3, "Should contain key3");
    }

    @Test
    void testOpenExistingSSTable() throws IOException {
        // Create an SSTable from the MemTable
        SSTable originalSSTable = new SSTable(memTable, tempDir.toString(), 0, 1);
        
        // Close the original SSTable
        originalSSTable.close();
        
        // Open the existing SSTable
        ssTable = new SSTable(tempDir.toString(), 0, 1);
        
        // Verify the SSTable was opened successfully
        assertNotNull(ssTable);
        assertEquals(0, ssTable.getLevel());
        assertEquals(1, ssTable.getSequenceNumber());
        
        // Test getting existing keys
        byte[] value1 = ssTable.get("key1".getBytes());
        assertNotNull(value1);
        assertArrayEquals("value1".getBytes(), value1);
    }

    @Test
    void testDelete() throws IOException {
        // Create an SSTable from the MemTable
        ssTable = new SSTable(memTable, tempDir.toString(), 0, 1);
        
        // Delete the SSTable
        boolean deleted = ssTable.delete();
        
        // Verify the SSTable was deleted successfully
        assertTrue(deleted);
    }
}