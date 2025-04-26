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

import com.umitunal.lsm.api.ByteArrayWrapper;
import com.umitunal.lsm.api.KeyValueIterator;
import com.umitunal.lsm.memtable.MemTable;
import com.umitunal.lsm.sstable.data.DataFileManager;
import com.umitunal.lsm.sstable.filter.FilterManager;
import com.umitunal.lsm.sstable.index.IndexFileManager;
import com.umitunal.lsm.sstable.io.SSTableIO;
import com.umitunal.lsm.sstable.iterator.SSTableIteratorFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the SSTableImpl class.
 */
@ExtendWith(MockitoExtension.class)
class SSTableImplTest {

    @Mock
    private DataFileManager mockDataFileManager;

    @Mock
    private IndexFileManager mockIndexFileManager;

    @Mock
    private FilterManager mockFilterManager;

    @Mock
    private SSTableIO mockIO;

    @Mock
    private SSTableIteratorFactory mockIteratorFactory;

    @Mock
    private MemTable mockMemTable;

    @Mock
    private FileChannel mockFileChannel;

    @Mock
    private KeyValueIterator mockIterator;

    private SSTableImpl ssTable;

    @BeforeEach
    void setUp() throws IOException {
        // Mock the IO methods that are called in the constructor
        when(mockIO.getLevel()).thenReturn(1);
        when(mockIO.getSequenceNumber()).thenReturn(123L);

        // Create the SSTable with mocked dependencies
        ssTable = new SSTableImpl(
                mockDataFileManager,
                mockIndexFileManager,
                mockFilterManager,
                mockIO,
                mockIteratorFactory);
    }

    @Test
    void testConstructorWithMemTable() throws IOException {
        // Create a new SSTable with a MemTable
        SSTableImpl sstableWithMemTable = new SSTableImpl(
                mockMemTable,
                mockDataFileManager,
                mockIndexFileManager,
                mockFilterManager,
                mockIO,
                mockIteratorFactory);

        // Verify that the IO's flushToDisk method was called with the MemTable
        verify(mockIO).flushToDisk(mockMemTable);

        // Verify that the level and sequence number were retrieved from the IO
        verify(mockIO, times(2)).getLevel();
        verify(mockIO, times(2)).getSequenceNumber();

        // Verify the SSTable properties
        assertEquals(1, sstableWithMemTable.getLevel());
        assertEquals(123L, sstableWithMemTable.getSequenceNumber());
    }

    @Test
    void testConstructorWithoutMemTable() throws IOException {
        // Verify that the IO's loadFromDisk method was called
        verify(mockIO).loadFromDisk();

        // Verify that the level and sequence number were retrieved from the IO
        verify(mockIO).getLevel();
        verify(mockIO).getSequenceNumber();

        // Verify the SSTable properties
        assertEquals(1, ssTable.getLevel());
        assertEquals(123L, ssTable.getSequenceNumber());
    }

    @Test
    void testGet() {
        // Arrange
        byte[] key = "testKey".getBytes();
        byte[] value = "testValue".getBytes();
        Map.Entry<ByteArrayWrapper, Long> indexEntry = new AbstractMap.SimpleEntry<>(new ByteArrayWrapper(key), 100L);

        when(mockFilterManager.mightContain(key)).thenReturn(true);
        when(mockIndexFileManager.findClosestKey(key)).thenReturn(indexEntry);
        try {
            when(mockDataFileManager.findKeyInDataFile(key, 100L)).thenReturn(value);
        } catch (IOException e) {
            fail("Unexpected exception: " + e.getMessage());
        }

        // Act
        byte[] result = ssTable.get(key);

        // Assert
        assertArrayEquals(value, result);
        verify(mockFilterManager).mightContain(key);
        verify(mockIndexFileManager).findClosestKey(key);
        try {
            verify(mockDataFileManager).findKeyInDataFile(key, 100L);
        } catch (IOException e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    void testGetWithNullKey() {
        // Act
        byte[] result = ssTable.get(null);

        // Assert
        assertNull(result);
        verify(mockFilterManager, never()).mightContain(any());
        verify(mockIndexFileManager, never()).findClosestKey(any());
        try {
            verify(mockDataFileManager, never()).findKeyInDataFile(any(), anyLong());
        } catch (IOException e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    void testGetWithEmptyKey() {
        // Act
        byte[] result = ssTable.get(new byte[0]);

        // Assert
        assertNull(result);
        verify(mockFilterManager, never()).mightContain(any());
        verify(mockIndexFileManager, never()).findClosestKey(any());
        try {
            verify(mockDataFileManager, never()).findKeyInDataFile(any(), anyLong());
        } catch (IOException e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    void testGetWithBloomFilterNegative() {
        // Arrange
        byte[] key = "testKey".getBytes();

        when(mockFilterManager.mightContain(key)).thenReturn(false);

        // Act
        byte[] result = ssTable.get(key);

        // Assert
        assertNull(result);
        verify(mockFilterManager).mightContain(key);
        verify(mockIndexFileManager, never()).findClosestKey(any());
        try {
            verify(mockDataFileManager, never()).findKeyInDataFile(any(), anyLong());
        } catch (IOException e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    void testGetWithNoIndexEntry() {
        // Arrange
        byte[] key = "testKey".getBytes();
        byte[] value = "testValue".getBytes();

        when(mockFilterManager.mightContain(key)).thenReturn(true);
        when(mockIndexFileManager.findClosestKey(key)).thenReturn(null);
        try {
            when(mockDataFileManager.findKeyInDataFile(key, 16)).thenReturn(value);
        } catch (IOException e) {
            fail("Unexpected exception: " + e.getMessage());
        }

        // Act
        byte[] result = ssTable.get(key);

        // Assert
        assertArrayEquals(value, result);
        verify(mockFilterManager).mightContain(key);
        verify(mockIndexFileManager).findClosestKey(key);
        try {
            verify(mockDataFileManager).findKeyInDataFile(key, 16);
        } catch (IOException e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    void testMightContain() {
        // Arrange
        byte[] key = "testKey".getBytes();
        when(mockFilterManager.mightContain(key)).thenReturn(true);

        // Act
        boolean result = ssTable.mightContain(key);

        // Assert
        assertTrue(result);
        verify(mockFilterManager).mightContain(key);
    }

    @Test
    void testGetSizeBytes() throws IOException {
        // Arrange
        when(mockDataFileManager.getDataChannel()).thenReturn(mockFileChannel);
        when(mockIndexFileManager.getIndexChannel()).thenReturn(mockFileChannel);
        when(mockFileChannel.size()).thenReturn(100L);

        // Act
        long result = ssTable.getSizeBytes();

        // Assert
        assertEquals(200L, result); // 100L for data file + 100L for index file
        verify(mockDataFileManager).getDataChannel();
        verify(mockIndexFileManager).getIndexChannel();
        verify(mockFileChannel, times(2)).size();
    }

    @Test
    void testDelete() throws IOException {
        // Arrange
        when(mockIO.deleteFiles()).thenReturn(true);

        // Act
        boolean result = ssTable.delete();

        // Assert
        assertTrue(result);
        verify(mockDataFileManager).close();
        verify(mockIndexFileManager).close();
        verify(mockIO).close();
        verify(mockIO).deleteFiles();
    }

    @Test
    void testListKeys() throws IOException {
        // Arrange
        List<byte[]> keys = List.of("key1".getBytes(), "key2".getBytes());
        when(mockDataFileManager.listKeys()).thenReturn(keys);

        // Act
        List<byte[]> result = ssTable.listKeys();

        // Assert
        assertEquals(keys, result);
        verify(mockDataFileManager).listKeys();
    }

    @Test
    void testCountEntries() throws IOException {
        // Arrange
        when(mockDataFileManager.countEntries()).thenReturn(10);

        // Act
        int result = ssTable.countEntries();

        // Assert
        assertEquals(10, result);
        verify(mockDataFileManager).countEntries();
    }

    @Test
    void testGetRange() throws IOException {
        // Arrange
        byte[] startKey = "start".getBytes();
        byte[] endKey = "end".getBytes();
        Map<byte[], byte[]> rangeMap = Map.of(
                "key1".getBytes(), "value1".getBytes(),
                "key2".getBytes(), "value2".getBytes()
        );
        when(mockDataFileManager.getRange(startKey, endKey)).thenReturn(rangeMap);

        // Act
        Map<byte[], byte[]> result = ssTable.getRange(startKey, endKey);

        // Assert
        assertEquals(rangeMap, result);
        verify(mockDataFileManager).getRange(startKey, endKey);
    }

    @Test
    void testClose() throws IOException {
        // Act
        ssTable.close();

        // Assert
        verify(mockDataFileManager).close();
        verify(mockIndexFileManager).close();
        verify(mockIO).close();
    }

    @Test
    void testGetIterator() throws IOException {
        // Arrange
        byte[] startKey = "start".getBytes();
        byte[] endKey = "end".getBytes();
        when(mockIteratorFactory.createIterator(startKey, endKey)).thenReturn(mockIterator);

        // Act
        KeyValueIterator result = ssTable.getIterator(startKey, endKey);

        // Assert
        assertSame(mockIterator, result);
        verify(mockIteratorFactory).createIterator(startKey, endKey);
    }
}
