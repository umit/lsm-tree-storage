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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the SSTableAdapter class.
 */
@ExtendWith(MockitoExtension.class)
class SSTableAdapterTest {

    @Mock
    private SSTableInterface mockDelegate;

    private SSTableAdapter adapter;

    @BeforeEach
    void setUp() throws IOException {
        adapter = new SSTableAdapter(mockDelegate);
    }

    @Test
    void testGet() {
        // Arrange
        byte[] key = "testKey".getBytes();
        byte[] value = "testValue".getBytes();
        when(mockDelegate.get(key)).thenReturn(value);

        // Act
        byte[] result = adapter.get(key);

        // Assert
        assertArrayEquals(value, result);
        verify(mockDelegate).get(key);
    }

    @Test
    void testMightContain() {
        // Arrange
        byte[] key = "testKey".getBytes();
        when(mockDelegate.mightContain(key)).thenReturn(true);

        // Act
        boolean result = adapter.mightContain(key);

        // Assert
        assertTrue(result);
        verify(mockDelegate).mightContain(key);
    }

    @Test
    void testGetLevel() {
        // Arrange
        int level = 1;
        when(mockDelegate.getLevel()).thenReturn(level);

        // Act
        int result = adapter.getLevel();

        // Assert
        assertEquals(level, result);
        verify(mockDelegate).getLevel();
    }

    @Test
    void testGetSequenceNumber() {
        // Arrange
        long sequenceNumber = 123;
        when(mockDelegate.getSequenceNumber()).thenReturn(sequenceNumber);

        // Act
        long result = adapter.getSequenceNumber();

        // Assert
        assertEquals(sequenceNumber, result);
        verify(mockDelegate).getSequenceNumber();
    }

    @Test
    void testGetCreationTime() {
        // Arrange
        long creationTime = System.currentTimeMillis();
        when(mockDelegate.getCreationTime()).thenReturn(creationTime);

        // Act
        long result = adapter.getCreationTime();

        // Assert
        assertEquals(creationTime, result);
        verify(mockDelegate).getCreationTime();
    }

    @Test
    void testGetSizeBytes() {
        // Arrange
        long sizeBytes = 1024;
        when(mockDelegate.getSizeBytes()).thenReturn(sizeBytes);

        // Act
        long result = adapter.getSizeBytes();

        // Assert
        assertEquals(sizeBytes, result);
        verify(mockDelegate).getSizeBytes();
    }

    @Test
    void testClose() throws IOException {
        // Act
        adapter.close();

        // Assert
        verify(mockDelegate).close();
    }

    @Test
    void testDelete() {
        // Arrange
        when(mockDelegate.delete()).thenReturn(true);

        // Act
        boolean result = adapter.delete();

        // Assert
        assertTrue(result);
        verify(mockDelegate).delete();
    }

    @Test
    void testListKeys() {
        // Arrange
        List<byte[]> keys = List.of("key1".getBytes(), "key2".getBytes());
        when(mockDelegate.listKeys()).thenReturn(keys);

        // Act
        List<byte[]> result = adapter.listKeys();

        // Assert
        assertEquals(keys, result);
        verify(mockDelegate).listKeys();
    }

    @Test
    void testCountEntries() {
        // Arrange
        int count = 10;
        when(mockDelegate.countEntries()).thenReturn(count);

        // Act
        int result = adapter.countEntries();

        // Assert
        assertEquals(count, result);
        verify(mockDelegate).countEntries();
    }

    @Test
    void testGetRange() {
        // Arrange
        byte[] startKey = "start".getBytes();
        byte[] endKey = "end".getBytes();
        Map<byte[], byte[]> rangeMap = Map.of(
                "key1".getBytes(), "value1".getBytes(),
                "key2".getBytes(), "value2".getBytes()
        );
        when(mockDelegate.getRange(startKey, endKey)).thenReturn(rangeMap);

        // Act
        Map<byte[], byte[]> result = adapter.getRange(startKey, endKey);

        // Assert
        assertEquals(rangeMap, result);
        verify(mockDelegate).getRange(startKey, endKey);
    }

    @Test
    void testGetDelegate() {
        // Act
        SSTableInterface result = adapter.getDelegate();

        // Assert
        assertSame(mockDelegate, result);
    }
}