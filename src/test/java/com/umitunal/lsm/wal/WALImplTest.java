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

package com.umitunal.lsm.wal;

import com.umitunal.lsm.wal.record.DeleteRecord;
import com.umitunal.lsm.wal.record.PutRecord;
import com.umitunal.lsm.wal.record.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the WALImpl class.
 */
class WALImplTest {
    private WAL wal;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        // Create a WAL in a temporary directory
        wal = new WALImpl(tempDir.toString());
    }

    @AfterEach
    void tearDown() throws IOException {
        // Close the WAL
        if (wal != null) {
            wal.close();
        }
    }

    @Test
    void testAppendAndReadPutRecord() throws IOException {
        // Append a put record
        byte[] key = "testKey".getBytes();
        byte[] value = "testValue".getBytes();
        long ttlSeconds = 60;

        long seqNum = wal.appendPutRecord(key, value, ttlSeconds);

        // Read all records
        List<Record> records = wal.readRecords();

        // Verify the record was read correctly
        assertEquals(1, records.size());
        assertTrue(records.get(0) instanceof PutRecord);

        PutRecord putRecord = (PutRecord) records.get(0);
        assertEquals(seqNum, putRecord.getSequenceNumber());
        assertArrayEquals(key, putRecord.getKey());
        assertArrayEquals(value, putRecord.getValue());
        assertEquals(ttlSeconds, putRecord.getTtlSeconds());
    }

    @Test
    void testAppendAndReadDeleteRecord() throws IOException {
        // Append a delete record
        byte[] key = "deleteKey".getBytes();

        long seqNum = wal.appendDeleteRecord(key);

        // Read all records
        List<Record> records = wal.readRecords();

        // Verify the record was read correctly
        assertEquals(1, records.size());
        assertTrue(records.get(0) instanceof DeleteRecord);

        DeleteRecord deleteRecord = (DeleteRecord) records.get(0);
        assertEquals(seqNum, deleteRecord.getSequenceNumber());
        assertArrayEquals(key, deleteRecord.getKey());
    }

    @Test
    void testMultipleRecords() throws IOException {
        // Append multiple records
        byte[] key1 = "key1".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] key2 = "key2".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] key3 = "key3".getBytes();

        long seqNum1 = wal.appendPutRecord(key1, value1, 0);
        long seqNum2 = wal.appendPutRecord(key2, value2, 120);
        long seqNum3 = wal.appendDeleteRecord(key3);

        // Read all records
        List<Record> records = wal.readRecords();

        // Verify the records were read correctly
        assertEquals(3, records.size());

        // First record
        assertTrue(records.get(0) instanceof PutRecord);
        PutRecord putRecord1 = (PutRecord) records.get(0);
        assertEquals(seqNum1, putRecord1.getSequenceNumber());
        assertArrayEquals(key1, putRecord1.getKey());
        assertArrayEquals(value1, putRecord1.getValue());
        assertEquals(0, putRecord1.getTtlSeconds());

        // Second record
        assertTrue(records.get(1) instanceof PutRecord);
        PutRecord putRecord2 = (PutRecord) records.get(1);
        assertEquals(seqNum2, putRecord2.getSequenceNumber());
        assertArrayEquals(key2, putRecord2.getKey());
        assertArrayEquals(value2, putRecord2.getValue());
        assertEquals(120, putRecord2.getTtlSeconds());

        // Third record
        assertTrue(records.get(2) instanceof DeleteRecord);
        DeleteRecord deleteRecord = (DeleteRecord) records.get(2);
        assertEquals(seqNum3, deleteRecord.getSequenceNumber());
        assertArrayEquals(key3, deleteRecord.getKey());
    }

    @Test
    void testDeleteAllLogs() throws IOException {
        // Append some records
        wal.appendPutRecord("key1".getBytes(), "value1".getBytes(), 0);
        wal.appendPutRecord("key2".getBytes(), "value2".getBytes(), 0);

        // Delete all logs
        wal.deleteAllLogs();

        // Verify all logs are deleted
        List<Record> records = wal.readRecords();
        assertEquals(0, records.size());
    }

    @Test
    void testRecoveryFromExistingLogs() throws IOException {
        // Append some records
        byte[] key1 = "key1".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] key2 = "key2".getBytes();
        byte[] value2 = "value2".getBytes();

        wal.appendPutRecord(key1, value1, 0);
        wal.appendPutRecord(key2, value2, 0);

        // Close the WAL
        wal.close();

        // Create a new WAL with the same directory
        WAL recoveredWal = new WALImpl(tempDir.toString());

        // Read records from the recovered WAL
        List<Record> records = recoveredWal.readRecords();

        // Verify the records were recovered correctly
        assertEquals(2, records.size());

        // First record
        assertTrue(records.get(0) instanceof PutRecord);
        PutRecord putRecord1 = (PutRecord) records.get(0);
        assertArrayEquals(key1, putRecord1.getKey());
        assertArrayEquals(value1, putRecord1.getValue());

        // Second record
        assertTrue(records.get(1) instanceof PutRecord);
        PutRecord putRecord2 = (PutRecord) records.get(1);
        assertArrayEquals(key2, putRecord2.getKey());
        assertArrayEquals(value2, putRecord2.getValue());

        // Clean up
        recoveredWal.close();
    }
}