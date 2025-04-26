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

import com.umitunal.lsm.wal.manager.WALManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Implementation of the WALWriter interface.
 * This class handles the writing of records to WAL files.
 */
public class WALWriterImpl implements WALWriter {
    private static final Logger logger = LoggerFactory.getLogger(WALWriterImpl.class);

    // Constants for record types
    private static final byte PUT_RECORD = 1;
    private static final byte DELETE_RECORD = 2;

    private final WALManager walManager;

    /**
     * Creates a new WALWriterImpl.
     * 
     * @param walManager the WAL manager
     */
    public WALWriterImpl(WALManager walManager) {
        this.walManager = walManager;
    }

    @Override
    public synchronized long appendPutRecord(byte[] key, byte[] value, long ttlSeconds) throws IOException {
        if (key == null || key.length == 0 || value == null) {
            throw new IllegalArgumentException("Key and value cannot be null or empty");
        }

        // Check if we need to rotate the log
        if (walManager.getCurrentFile().size() >= walManager.getMaxLogSizeBytes()) {
            walManager.rotateLog();
        }

        // Get the next sequence number
        long seqNum = walManager.getNextSequenceNumber();

        // Calculate the record size
        int recordSize = 1 + 8 + 4 + key.length + 4 + value.length + 8;

        // Create a buffer for the record
        ByteBuffer buffer = ByteBuffer.allocate(recordSize);

        // Write the record type
        buffer.put(PUT_RECORD);

        // Write the sequence number
        buffer.putLong(seqNum);

        // Write the key length and key
        buffer.putInt(key.length);
        buffer.put(key);

        // Write the value length and value
        buffer.putInt(value.length);
        buffer.put(value);

        // Write the TTL
        buffer.putLong(ttlSeconds);

        // Flip the buffer for writing
        buffer.flip();

        // Write the buffer to the log
        walManager.getCurrentFile().write(buffer);

        // Force the data to disk
        walManager.getCurrentFile().force(true);

        logger.debug("Appended put record with sequence number: " + seqNum);

        return seqNum;
    }

    @Override
    public synchronized long appendDeleteRecord(byte[] key) throws IOException {
        if (key == null || key.length == 0) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }

        // Check if we need to rotate the log
        if (walManager.getCurrentFile().size() >= walManager.getMaxLogSizeBytes()) {
            walManager.rotateLog();
        }

        // Get the next sequence number
        long seqNum = walManager.getNextSequenceNumber();

        // Calculate the record size
        int recordSize = 1 + 8 + 4 + key.length;

        // Create a buffer for the record
        ByteBuffer buffer = ByteBuffer.allocate(recordSize);

        // Write the record type
        buffer.put(DELETE_RECORD);

        // Write the sequence number
        buffer.putLong(seqNum);

        // Write the key length and key
        buffer.putInt(key.length);
        buffer.put(key);

        // Flip the buffer for writing
        buffer.flip();

        // Write the buffer to the log
        walManager.getCurrentFile().write(buffer);

        // Force the data to disk
        walManager.getCurrentFile().force(true);

        logger.debug("Appended delete record with sequence number: " + seqNum);

        return seqNum;
    }
}
