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

package com.umitunal.lsm.wal.manager;

import com.umitunal.lsm.wal.file.WALFile;
import com.umitunal.lsm.wal.file.WALFileImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of the WALManager interface.
 * This class handles the management of WAL files.
 */
public class WALManagerImpl implements WALManager {
    private static final Logger logger = LoggerFactory.getLogger(WALManagerImpl.class);

    // Directory to store WAL files
    private final String directory;

    // Current WAL file
    private WALFile currentFile;

    // Sequence number for records
    private final AtomicLong sequenceNumber;

    // Maximum size of a WAL file before rotation
    private final long maxLogSizeBytes;

    /**
     * Creates a new WALManagerImpl.
     * 
     * @param directory the directory to store WAL files
     * @throws IOException if an I/O error occurs
     */
    public WALManagerImpl(String directory) throws IOException {
        this(directory, 64 * 1024 * 1024); // Default 64MB max log size
    }

    /**
     * Creates a new WALManagerImpl with a custom maximum log size.
     * 
     * @param directory the directory to store WAL files
     * @param maxLogSizeBytes maximum size of a WAL file before rotation
     * @throws IOException if an I/O error occurs
     */
    public WALManagerImpl(String directory, long maxLogSizeBytes) throws IOException {
        this.directory = directory;
        this.maxLogSizeBytes = maxLogSizeBytes;
        this.sequenceNumber = new AtomicLong(0);

        // Create directory if it doesn't exist
        Files.createDirectories(Paths.get(directory));

        // Initialize the current log file
        initializeCurrentLog();

        logger.info("WALManager initialized in directory: " + directory);
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
            createNewFile();
        } else {
            // Use the latest log file
            Path latestLogPath = logFiles.get(logFiles.size() - 1);

            // Update sequence number based on the log file name
            String fileName = latestLogPath.getFileName().toString();
            try {
                long fileSeqNum = Long.parseLong(fileName.substring(4, fileName.indexOf(".log")));
                sequenceNumber.set(fileSeqNum);

                // Open the latest log file
                currentFile = new WALFileImpl(latestLogPath, fileSeqNum, 
                        StandardOpenOption.READ, StandardOpenOption.WRITE);
            } catch (NumberFormatException | IndexOutOfBoundsException e) {
                logger.warn("Could not parse sequence number from log file name: " + fileName, e);
                // Create a new log file as a fallback
                createNewFile();
            }
        }
    }

    @Override
    public WALFile getCurrentFile() {
        return currentFile;
    }

    @Override
    public WALFile createNewFile() throws IOException {
        long seqNum = sequenceNumber.getAndIncrement();
        Path newLogPath = Paths.get(directory, String.format("wal_%020d.log", seqNum));

        // Close the current file if it exists
        if (currentFile != null) {
            currentFile.close();
        }

        // Create a new file
        currentFile = new WALFileImpl(newLogPath, seqNum, 
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ);

        logger.info("Created new WAL file: " + newLogPath);

        return currentFile;
    }

    @Override
    public List<Path> findLogFiles() throws IOException {
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

    @Override
    public void rotateLog() throws IOException {
        // Create a new log file
        createNewFile();

        logger.info("Rotated WAL to new file");
    }

    @Override
    public void deleteAllLogs() throws IOException {
        // Close the current log file
        if (currentFile != null) {
            currentFile.close();
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
        createNewFile();

        logger.info("Deleted all WAL files");
    }

    @Override
    public String getDirectory() {
        return directory;
    }

    @Override
    public long getMaxLogSizeBytes() {
        return maxLogSizeBytes;
    }

    @Override
    public long getNextSequenceNumber() {
        return sequenceNumber.getAndIncrement();
    }
}
