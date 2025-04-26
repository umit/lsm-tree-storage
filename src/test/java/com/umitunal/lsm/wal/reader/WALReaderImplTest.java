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

package com.umitunal.lsm.wal.reader;

import com.umitunal.lsm.wal.manager.WALManager;
import com.umitunal.lsm.wal.record.DeleteRecord;
import com.umitunal.lsm.wal.record.PutRecord;
import com.umitunal.lsm.wal.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

/**
 * Tests for the WALReaderImpl class.
 */
class WALReaderImplTest {

    @TempDir
    Path tempDir;

    @Mock
    private WALManager mockWalManager;

    private WALReaderImpl reader;
    private Path walFile1;
    private Path walFile2;

    @BeforeEach
    void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);
        reader = new WALReaderImpl(mockWalManager);

        // Create test WAL files
        walFile1 = tempDir.resolve("wal_00000000000000000001.log");
        walFile2 = tempDir.resolve("wal_00000000000000000002.log");

        // Create file 1 with a PUT record
        createPutRecord(walFile1, 1, "key1", "value1", 0);

        // Create file 2 with a DELETE record
        createDeleteRecord(walFile2, 2, "key2");

        // Mock the WALManager to return our test files
        when(mockWalManager.findLogFiles()).thenReturn(Arrays.asList(walFile1, walFile2));
    }

    @Test
    void testReadRecords() throws IOException {
        // Read all records
        List<Record> records = reader.readRecords();

        // Verify we got both records
        assertEquals(2, records.size());

        // Verify the first record is a PutRecord
        assertTrue(records.get(0) instanceof PutRecord);
        PutRecord putRecord = (PutRecord) records.get(0);
        assertEquals(1, putRecord.getSequenceNumber());
        assertArrayEquals("key1".getBytes(), putRecord.getKey());
        assertArrayEquals("value1".getBytes(), putRecord.getValue());
        assertEquals(0, putRecord.getTtlSeconds());

        // Verify the second record is a DeleteRecord
        assertTrue(records.get(1) instanceof DeleteRecord);
        DeleteRecord deleteRecord = (DeleteRecord) records.get(1);
        assertEquals(2, deleteRecord.getSequenceNumber());
        assertArrayEquals("key2".getBytes(), deleteRecord.getKey());
    }

    @Test
    void testReadRecordsFromFile() throws IOException {
        // Read records from file 1
        List<Record> records = reader.readRecordsFromFile(walFile1.toString());

        // Verify we got one record
        assertEquals(1, records.size());

        // Verify it's a PutRecord
        assertTrue(records.get(0) instanceof PutRecord);
        PutRecord putRecord = (PutRecord) records.get(0);
        assertEquals(1, putRecord.getSequenceNumber());
        assertArrayEquals("key1".getBytes(), putRecord.getKey());
        assertArrayEquals("value1".getBytes(), putRecord.getValue());
        assertEquals(0, putRecord.getTtlSeconds());

        // Read records from file 2
        records = reader.readRecordsFromFile(walFile2.toString());

        // Verify we got one record
        assertEquals(1, records.size());

        // Verify it's a DeleteRecord
        assertTrue(records.get(0) instanceof DeleteRecord);
        DeleteRecord deleteRecord = (DeleteRecord) records.get(0);
        assertEquals(2, deleteRecord.getSequenceNumber());
        assertArrayEquals("key2".getBytes(), deleteRecord.getKey());
    }

    @Test
    void testReadRecordsFromNonExistentFile() {
        // Try to read records from a non-existent file
        assertThrows(IOException.class, () -> {
            reader.readRecordsFromFile(tempDir.resolve("non-existent.log").toString());
        });
    }

    /**
     * Helper method to create a PUT record in a WAL file.
     */
    private void createPutRecord(Path filePath, long seqNum, String key, String value, long ttlSeconds) throws IOException {
        try (FileChannel channel = FileChannel.open(filePath, 
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            
            byte[] keyBytes = key.getBytes();
            byte[] valueBytes = value.getBytes();
            
            // Calculate the record size
            int recordSize = 1 + 8 + 4 + keyBytes.length + 4 + valueBytes.length + 8;
            
            // Create a buffer for the record
            ByteBuffer buffer = ByteBuffer.allocate(recordSize);
            
            // Write the record type (1 for PUT)
            buffer.put((byte) 1);
            
            // Write the sequence number
            buffer.putLong(seqNum);
            
            // Write the key length and key
            buffer.putInt(keyBytes.length);
            buffer.put(keyBytes);
            
            // Write the value length and value
            buffer.putInt(valueBytes.length);
            buffer.put(valueBytes);
            
            // Write the TTL
            buffer.putLong(ttlSeconds);
            
            // Flip the buffer for writing
            buffer.flip();
            
            // Write the buffer to the file
            channel.write(buffer);
        }
    }

    /**
     * Helper method to create a DELETE record in a WAL file.
     */
    private void createDeleteRecord(Path filePath, long seqNum, String key) throws IOException {
        try (FileChannel channel = FileChannel.open(filePath, 
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            
            byte[] keyBytes = key.getBytes();
            
            // Calculate the record size
            int recordSize = 1 + 8 + 4 + keyBytes.length;
            
            // Create a buffer for the record
            ByteBuffer buffer = ByteBuffer.allocate(recordSize);
            
            // Write the record type (2 for DELETE)
            buffer.put((byte) 2);
            
            // Write the sequence number
            buffer.putLong(seqNum);
            
            // Write the key length and key
            buffer.putInt(keyBytes.length);
            buffer.put(keyBytes);
            
            // Flip the buffer for writing
            buffer.flip();
            
            // Write the buffer to the file
            channel.write(buffer);
        }
    }
}