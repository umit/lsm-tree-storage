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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of the WALReader interface.
 * This class handles the reading of records from WAL files.
 */
public class WALReaderImpl implements WALReader {
    private static final Logger logger = LoggerFactory.getLogger(WALReaderImpl.class);

    // Constants for record types
    private static final byte PUT_RECORD = 1;
    private static final byte DELETE_RECORD = 2;

    private final WALManager walManager;

    /**
     * Creates a new WALReaderImpl.
     * 
     * @param walManager the WAL manager
     */
    public WALReaderImpl(WALManager walManager) {
        this.walManager = walManager;
    }

    @Override
    public List<Record> readRecords() throws IOException {
        List<Record> records = new ArrayList<>();

        // Find all log files
        List<Path> logFiles = walManager.findLogFiles();

        // Read records from each log file
        for (Path logPath : logFiles) {
            records.addAll(readRecordsFromFile(logPath.toString()));
        }

        return records;
    }

    @Override
    public List<Record> readRecordsFromFile(String filePath) throws IOException {
        List<Record> records = new ArrayList<>();

        try (FileChannel channel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate(1024); // Initial buffer size

            while (channel.position() < channel.size()) {
                // Read the record type
                buffer.clear();
                buffer.limit(1);
                channel.read(buffer);
                buffer.flip();
                byte recordType = buffer.get();

                // Read the sequence number
                buffer.clear();
                buffer.limit(8);
                channel.read(buffer);
                buffer.flip();
                long seqNum = buffer.getLong();

                // Read the key length
                buffer.clear();
                buffer.limit(4);
                channel.read(buffer);
                buffer.flip();
                int keyLength = buffer.getInt();

                // Read the key
                byte[] key = new byte[keyLength];
                buffer.clear();
                buffer.limit(keyLength);
                if (buffer.capacity() < keyLength) {
                    buffer = ByteBuffer.allocate(keyLength);
                }
                channel.read(buffer);
                buffer.flip();
                buffer.get(key);

                if (recordType == PUT_RECORD) {
                    // Read the value length
                    buffer.clear();
                    buffer.limit(4);
                    channel.read(buffer);
                    buffer.flip();
                    int valueLength = buffer.getInt();

                    // Read the value
                    byte[] value = new byte[valueLength];
                    buffer.clear();
                    buffer.limit(valueLength);
                    if (buffer.capacity() < valueLength) {
                        buffer = ByteBuffer.allocate(valueLength);
                    }
                    channel.read(buffer);
                    buffer.flip();
                    buffer.get(value);

                    // Read the TTL
                    buffer.clear();
                    buffer.limit(8);
                    channel.read(buffer);
                    buffer.flip();
                    long ttlSeconds = buffer.getLong();

                    // Create a put record
                    records.add(new PutRecord(seqNum, key, value, ttlSeconds));
                } else if (recordType == DELETE_RECORD) {
                    // Create a delete record
                    records.add(new DeleteRecord(seqNum, key));
                } else {
                    throw new IOException("Unknown record type: " + recordType);
                }
            }
        } catch (IOException e) {
            logger.error("Error reading records from file: " + filePath, e);
            throw e;
        }

        return records;
    }
}
