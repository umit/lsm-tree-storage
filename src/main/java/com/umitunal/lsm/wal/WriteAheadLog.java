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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Write-Ahead Log (WAL) implementation for the LSM-tree storage.
 * The WAL provides durability by logging operations before they are applied to the MemTable.
 * In case of a crash, the WAL can be replayed to recover the state of the MemTable.
 */
public class WriteAheadLog implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(WriteAheadLog.class);

    // Constants for record types
    private static final byte PUT_RECORD = 1;
    private static final byte DELETE_RECORD = 2;

    // Directory to store WAL files
    private final String directory;

    // Current WAL file
    private Path currentLogPath;
    private FileChannel currentLogChannel;

    // Sequence number for records
    private final AtomicLong sequenceNumber;

    // Maximum size of a WAL file before rotation
    private final long maxLogSizeBytes;

    /**
     * Creates a new WriteAheadLog.
     * 
     * @param directory the directory to store WAL files
     * @throws IOException if an I/O error occurs
     */
    public WriteAheadLog(String directory) throws IOException {
        this(directory, 64 * 1024 * 1024); // Default 64MB max log size
    }

    /**
     * Creates a new WriteAheadLog with a custom maximum log size.
     * 
     * @param directory the directory to store WAL files
     * @param maxLogSizeBytes maximum size of a WAL file before rotation
     * @throws IOException if an I/O error occurs
     */
    public WriteAheadLog(String directory, long maxLogSizeBytes) throws IOException {
        this.directory = directory;
        this.maxLogSizeBytes = maxLogSizeBytes;
        this.sequenceNumber = new AtomicLong(0);

        // Create directory if it doesn't exist
        Files.createDirectories(Paths.get(directory));

        // Initialize the current log file
        initializeCurrentLog();

        logger.info("WriteAheadLog initialized in directory: " + directory);
    }

    /**
     * Initializes the current log file.
     * If there are existing log files, it uses the latest one.
     * Otherwise, it creates a new log file.
     * 
     * @throws IOException if an I/O error occurs
     */
    private void initializeCurrentLog() throws IOException {
        // Find the latest log file
        List<Path> logFiles = findLogFiles();

        if (logFiles.isEmpty()) {
            // Create a new log file
            createNewLogFile();
        } else {
            // Use the latest log file
            currentLogPath = logFiles.get(logFiles.size() - 1);
            currentLogChannel = FileChannel.open(currentLogPath, 
                    StandardOpenOption.READ, StandardOpenOption.WRITE);

            // Update sequence number based on the log file name
            String fileName = currentLogPath.getFileName().toString();
            try {
                long fileSeqNum = Long.parseLong(fileName.substring(4, fileName.indexOf(".log")));
                sequenceNumber.set(fileSeqNum);
            } catch (NumberFormatException | IndexOutOfBoundsException e) {
                logger.warn("Could not parse sequence number from log file name: " + fileName, e);
            }
        }
    }

    /**
     * Creates a new log file.
     * 
     * @throws IOException if an I/O error occurs
     */
    private void createNewLogFile() throws IOException {
        long seqNum = sequenceNumber.getAndIncrement();
        currentLogPath = Paths.get(directory, String.format("wal_%020d.log", seqNum));
        currentLogChannel = FileChannel.open(currentLogPath, 
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ);

        logger.info("Created new WAL file: " + currentLogPath);
    }

    /**
     * Finds all WAL files in the directory.
     * 
     * @return a list of WAL files sorted by sequence number
     * @throws IOException if an I/O error occurs
     */
    private List<Path> findLogFiles() throws IOException {
        List<Path> logFiles = new ArrayList<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(directory), "wal_*.log")) {
            for (Path path : stream) {
                logFiles.add(path);
            }
        }

        // Sort by sequence number (extracted from file name)
        logFiles.sort((p1, p2) -> {
            String name1 = p1.getFileName().toString();
            String name2 = p2.getFileName().toString();

            try {
                long seq1 = Long.parseLong(name1.substring(4, name1.indexOf(".log")));
                long seq2 = Long.parseLong(name2.substring(4, name2.indexOf(".log")));
                return Long.compare(seq1, seq2);
            } catch (NumberFormatException | IndexOutOfBoundsException e) {
                logger.warn("Error parsing sequence numbers from log file names", e);
                return name1.compareTo(name2);
            }
        });

        return logFiles;
    }

    /**
     * Appends a put record to the WAL.
     * 
     * @param key the key as byte array
     * @param value the value as byte array
     * @param ttlSeconds time-to-live in seconds, 0 means no expiration
     * @return the sequence number of the record
     * @throws IOException if an I/O error occurs
     */
    public synchronized long appendPutRecord(byte[] key, byte[] value, long ttlSeconds) throws IOException {
        if (key == null || key.length == 0 || value == null) {
            throw new IllegalArgumentException("Key and value cannot be null or empty");
        }

        // Check if we need to rotate the log
        if (currentLogChannel.size() >= maxLogSizeBytes) {
            rotateLog();
        }

        // Get the next sequence number
        long seqNum = sequenceNumber.getAndIncrement();

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
        currentLogChannel.write(buffer);

        // Force the data to disk
        currentLogChannel.force(true);

        logger.debug("Appended put record with sequence number: " + seqNum);

        return seqNum;
    }

    /**
     * Appends a delete record to the WAL.
     * 
     * @param key the key as byte array
     * @return the sequence number of the record
     * @throws IOException if an I/O error occurs
     */
    public synchronized long appendDeleteRecord(byte[] key) throws IOException {
        if (key == null || key.length == 0) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }

        // Check if we need to rotate the log
        if (currentLogChannel.size() >= maxLogSizeBytes) {
            rotateLog();
        }

        // Get the next sequence number
        long seqNum = sequenceNumber.getAndIncrement();

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
        currentLogChannel.write(buffer);

        // Force the data to disk
        currentLogChannel.force(true);

        logger.debug("Appended delete record with sequence number: " + seqNum);

        return seqNum;
    }

    /**
     * Rotates the log by creating a new log file.
     * 
     * @throws IOException if an I/O error occurs
     */
    private void rotateLog() throws IOException {
        // Close the current log file
        currentLogChannel.close();

        // Create a new log file
        createNewLogFile();

        logger.info("Rotated WAL to new file: " + currentLogPath);
    }

    /**
     * Reads all records from all WAL files.
     * 
     * @return a list of records
     * @throws IOException if an I/O error occurs
     */
    public synchronized List<Record> readRecords() throws IOException {
        List<Record> records = new ArrayList<>();

        // Find all log files
        List<Path> logFiles = findLogFiles();

        // Read records from each log file
        for (Path logPath : logFiles) {
            try (FileChannel channel = FileChannel.open(logPath, StandardOpenOption.READ)) {
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
            }
        }

        return records;
    }

    @Override
    public void close() throws IOException {
        if (currentLogChannel != null && currentLogChannel.isOpen()) {
            currentLogChannel.close();
        }

        logger.info("WriteAheadLog closed");
    }

    /**
     * Deletes all WAL files.
     * 
     * @throws IOException if an I/O error occurs
     */
    public synchronized void deleteAllLogs() throws IOException {
        // Close the current log file
        if (currentLogChannel != null && currentLogChannel.isOpen()) {
            currentLogChannel.close();
        }

        // Find all log files
        List<Path> logFiles = findLogFiles();

        // Delete each log file
        for (Path logPath : logFiles) {
            try {
                Files.delete(logPath);
                logger.info("Deleted WAL file: " + logPath);
            } catch (IOException e) {
                logger.warn("Failed to delete WAL file: " + logPath, e);
            }
        }

        // Create a new log file
        createNewLogFile();

        logger.info("Deleted all WAL files");
    }

    /**
     * Interface for WAL records.
     */
    public interface Record {
        long getSequenceNumber();
        byte[] getKey();
    }

    /**
     * Put record implementation.
     */
    public class PutRecord implements Record {
        private final long sequenceNumber;
        private final byte[] key;
        public final byte[] value;
        public final long ttlSeconds;

        public PutRecord(long sequenceNumber, byte[] key, byte[] value, long ttlSeconds) {
            this.sequenceNumber = sequenceNumber;
            this.key = key;
            this.value = value;
            this.ttlSeconds = ttlSeconds;
        }

        @Override
        public long getSequenceNumber() {
            return sequenceNumber;
        }

        @Override
        public byte[] getKey() {
            return key;
        }
    }

    /**
     * Delete record implementation.
     */
    public class DeleteRecord implements Record {
        private final long sequenceNumber;
        private final byte[] key;

        public DeleteRecord(long sequenceNumber, byte[] key) {
            this.sequenceNumber = sequenceNumber;
            this.key = key;
        }

        @Override
        public long getSequenceNumber() {
            return sequenceNumber;
        }

        @Override
        public byte[] getKey() {
            return key;
        }
    }
}
