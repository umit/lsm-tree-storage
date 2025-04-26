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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the WriteAheadLog class.
 */
class WriteAheadLogTest {
    private WriteAheadLog wal;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        // Create a WAL in a temporary directory
        wal = new WriteAheadLog(tempDir.toString());
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
        List<WriteAheadLog.Record> records = wal.readRecords();

        // Verify the record was read correctly
        assertEquals(1, records.size());
        assertTrue(records.get(0) instanceof WriteAheadLog.PutRecord);

        WriteAheadLog.PutRecord putRecord = (WriteAheadLog.PutRecord) records.get(0);
        assertEquals(seqNum, putRecord.getSequenceNumber());
        assertArrayEquals(key, putRecord.getKey());
        assertArrayEquals(value, putRecord.value);
        assertEquals(ttlSeconds, putRecord.ttlSeconds);
    }

    @Test
    void testAppendAndReadDeleteRecord() throws IOException {
        // Append a delete record
        byte[] key = "deleteKey".getBytes();

        long seqNum = wal.appendDeleteRecord(key);

        // Read all records
        List<WriteAheadLog.Record> records = wal.readRecords();

        // Verify the record was read correctly
        assertEquals(1, records.size());
        assertTrue(records.get(0) instanceof WriteAheadLog.DeleteRecord);

        WriteAheadLog.DeleteRecord deleteRecord = (WriteAheadLog.DeleteRecord) records.get(0);
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
        List<WriteAheadLog.Record> records = wal.readRecords();

        // Verify the records were read correctly
        assertEquals(3, records.size());

        // First record
        assertTrue(records.get(0) instanceof WriteAheadLog.PutRecord);
        WriteAheadLog.PutRecord putRecord1 = (WriteAheadLog.PutRecord) records.get(0);
        assertEquals(seqNum1, putRecord1.getSequenceNumber());
        assertArrayEquals(key1, putRecord1.getKey());
        assertArrayEquals(value1, putRecord1.value);
        assertEquals(0, putRecord1.ttlSeconds);

        // Second record
        assertTrue(records.get(1) instanceof WriteAheadLog.PutRecord);
        WriteAheadLog.PutRecord putRecord2 = (WriteAheadLog.PutRecord) records.get(1);
        assertEquals(seqNum2, putRecord2.getSequenceNumber());
        assertArrayEquals(key2, putRecord2.getKey());
        assertArrayEquals(value2, putRecord2.value);
        assertEquals(120, putRecord2.ttlSeconds);

        // Third record
        assertTrue(records.get(2) instanceof WriteAheadLog.DeleteRecord);
        WriteAheadLog.DeleteRecord deleteRecord = (WriteAheadLog.DeleteRecord) records.get(2);
        assertEquals(seqNum3, deleteRecord.getSequenceNumber());
        assertArrayEquals(key3, deleteRecord.getKey());
    }

    @Test
    void testDeleteAllLogs() throws IOException {
        // Append some records
        wal.appendPutRecord("key1".getBytes(), "value1".getBytes(), 0);
        wal.appendPutRecord("key2".getBytes(), "value2".getBytes(), 0);

        // Verify records exist
        List<WriteAheadLog.Record> records = wal.readRecords();
        assertEquals(2, records.size());

        // Delete all logs
        wal.deleteAllLogs();

        // Verify no records exist
        records = wal.readRecords();
        assertEquals(0, records.size());

        // Verify we can still append records
        wal.appendPutRecord("key3".getBytes(), "value3".getBytes(), 0);
        records = wal.readRecords();
        assertEquals(1, records.size());
    }

    @Test
    void testLogRotation() throws IOException {
        // Create a WAL with a very small max log size to force rotation
        try (WriteAheadLog smallWal = new WriteAheadLog(tempDir.toString(), 100)) {
            // Append records until rotation occurs
            for (int i = 0; i < 10; i++) {
                byte[] key = ("key" + i).getBytes();
                byte[] value = ("value" + i).getBytes();
                smallWal.appendPutRecord(key, value, 0);
            }

            // Verify multiple log files were created
            long logFileCount = Files.list(tempDir)
                    .filter(p -> p.getFileName().toString().startsWith("wal_"))
                    .count();

            assertTrue(logFileCount > 1, "Multiple log files should be created");

            // Read all records
            List<WriteAheadLog.Record> records = smallWal.readRecords();

            // Verify all records were read correctly
            assertEquals(10, records.size());

            for (int i = 0; i < 10; i++) {
                assertTrue(records.get(i) instanceof WriteAheadLog.PutRecord);
                WriteAheadLog.PutRecord putRecord = (WriteAheadLog.PutRecord) records.get(i);
                assertArrayEquals(("key" + i).getBytes(), putRecord.getKey());
                assertArrayEquals(("value" + i).getBytes(), putRecord.value);
            }
        }
    }

    @Test
    void testRecovery() throws IOException {
        // Append some records
        byte[] key1 = "key1".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] key2 = "key2".getBytes();
        byte[] value2 = "value2".getBytes();

        wal.appendPutRecord(key1, value1, 0);
        wal.appendPutRecord(key2, value2, 0);

        // Close the WAL
        wal.close();

        // Create a new WAL instance (simulating recovery)
        WriteAheadLog recoveredWal = new WriteAheadLog(tempDir.toString());

        // Read all records
        List<WriteAheadLog.Record> records = recoveredWal.readRecords();

        // Verify the records were read correctly
        assertEquals(2, records.size());

        // First record
        assertTrue(records.get(0) instanceof WriteAheadLog.PutRecord);
        WriteAheadLog.PutRecord putRecord1 = (WriteAheadLog.PutRecord) records.get(0);
        assertArrayEquals(key1, putRecord1.getKey());
        assertArrayEquals(value1, putRecord1.value);

        // Second record
        assertTrue(records.get(1) instanceof WriteAheadLog.PutRecord);
        WriteAheadLog.PutRecord putRecord2 = (WriteAheadLog.PutRecord) records.get(1);
        assertArrayEquals(key2, putRecord2.getKey());
        assertArrayEquals(value2, putRecord2.value);

        // Clean up
        recoveredWal.close();
    }
}
