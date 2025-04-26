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

package com.umitunal.lsm.wal.writer;

import com.umitunal.lsm.wal.file.WALFile;
import com.umitunal.lsm.wal.manager.WALManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the WALWriterImpl class.
 */
class WALWriterImplTest {

    @Mock
    private WALManager mockWalManager;

    @Mock
    private WALFile mockWalFile;

    private WALWriterImpl writer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        writer = new WALWriterImpl(mockWalManager);

        // Mock the WALManager to return our mock WALFile
        when(mockWalManager.getCurrentFile()).thenReturn(mockWalFile);
    }

    @Test
    void testAppendPutRecord() throws IOException {
        // Arrange
        byte[] key = "testKey".getBytes();
        byte[] value = "testValue".getBytes();
        long ttlSeconds = 60;
        long seqNum = 123;

        // Mock the WALManager to return our sequence number
        when(mockWalManager.getNextSequenceNumber()).thenReturn(seqNum);

        // Act
        long result = writer.appendPutRecord(key, value, ttlSeconds);

        // Assert
        assertEquals(seqNum, result);

        // Verify the WALManager was called to get the current file and sequence number
        verify(mockWalManager, times(3)).getCurrentFile();
        verify(mockWalManager).getNextSequenceNumber();

        // Verify the WALFile was called to write the buffer and force the data to disk
        ArgumentCaptor<ByteBuffer> bufferCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        verify(mockWalFile).write(bufferCaptor.capture());
        verify(mockWalFile).force(true);

        // Verify the buffer contains the correct data
        ByteBuffer buffer = bufferCaptor.getValue();
        buffer.rewind(); // Reset position to beginning

        // Record type (1 for PUT)
        assertEquals(1, buffer.get());

        // Sequence number
        assertEquals(seqNum, buffer.getLong());

        // Key length and key
        assertEquals(key.length, buffer.getInt());
        byte[] keyBytes = new byte[key.length];
        buffer.get(keyBytes);
        assertArrayEquals(key, keyBytes);

        // Value length and value
        assertEquals(value.length, buffer.getInt());
        byte[] valueBytes = new byte[value.length];
        buffer.get(valueBytes);
        assertArrayEquals(value, valueBytes);

        // TTL
        assertEquals(ttlSeconds, buffer.getLong());
    }

    @Test
    void testAppendDeleteRecord() throws IOException {
        // Arrange
        byte[] key = "testKey".getBytes();
        long seqNum = 123;

        // Mock the WALManager to return our sequence number
        when(mockWalManager.getNextSequenceNumber()).thenReturn(seqNum);

        // Act
        long result = writer.appendDeleteRecord(key);

        // Assert
        assertEquals(seqNum, result);

        // Verify the WALManager was called to get the current file and sequence number
        verify(mockWalManager, times(3)).getCurrentFile();
        verify(mockWalManager).getNextSequenceNumber();

        // Verify the WALFile was called to write the buffer and force the data to disk
        ArgumentCaptor<ByteBuffer> bufferCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        verify(mockWalFile).write(bufferCaptor.capture());
        verify(mockWalFile).force(true);

        // Verify the buffer contains the correct data
        ByteBuffer buffer = bufferCaptor.getValue();
        buffer.rewind(); // Reset position to beginning

        // Record type (2 for DELETE)
        assertEquals(2, buffer.get());

        // Sequence number
        assertEquals(seqNum, buffer.getLong());

        // Key length and key
        assertEquals(key.length, buffer.getInt());
        byte[] keyBytes = new byte[key.length];
        buffer.get(keyBytes);
        assertArrayEquals(key, keyBytes);
    }

    @Test
    void testAppendPutRecordWithNullKey() {
        // Act & Assert
        assertThrows(IllegalArgumentException.class, () -> {
            writer.appendPutRecord(null, "value".getBytes(), 0);
        });
    }

    @Test
    void testAppendPutRecordWithEmptyKey() {
        // Act & Assert
        assertThrows(IllegalArgumentException.class, () -> {
            writer.appendPutRecord(new byte[0], "value".getBytes(), 0);
        });
    }

    @Test
    void testAppendPutRecordWithNullValue() {
        // Act & Assert
        assertThrows(IllegalArgumentException.class, () -> {
            writer.appendPutRecord("key".getBytes(), null, 0);
        });
    }

    @Test
    void testAppendDeleteRecordWithNullKey() {
        // Act & Assert
        assertThrows(IllegalArgumentException.class, () -> {
            writer.appendDeleteRecord(null);
        });
    }

    @Test
    void testAppendDeleteRecordWithEmptyKey() {
        // Act & Assert
        assertThrows(IllegalArgumentException.class, () -> {
            writer.appendDeleteRecord(new byte[0]);
        });
    }

    @Test
    void testLogRotationWhenSizeLimitExceeded() throws IOException {
        // Arrange
        byte[] key = "testKey".getBytes();
        byte[] value = "testValue".getBytes();
        long ttlSeconds = 60;
        long seqNum = 123;

        // Mock the WALManager to return our sequence number
        when(mockWalManager.getNextSequenceNumber()).thenReturn(seqNum);

        // Mock the WALFile to return a size larger than the max size
        when(mockWalFile.size()).thenReturn(Long.MAX_VALUE);

        // Mock the WALManager to return a max size
        when(mockWalManager.getMaxLogSizeBytes()).thenReturn(1024L);

        // Act
        writer.appendPutRecord(key, value, ttlSeconds);

        // Assert
        // Verify the WALManager was called to rotate the log
        verify(mockWalManager).rotateLog();
    }
}
