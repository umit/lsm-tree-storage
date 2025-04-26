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
 * Tests for the WALManagerImpl class.
 */
class WALManagerImplTest {

    @TempDir
    Path tempDir;

    private WALManagerImpl manager;
    private final long maxLogSizeBytes = 1024; // 1KB for testing

    @BeforeEach
    void setUp() throws IOException {
        manager = new WALManagerImpl(tempDir.toString(), maxLogSizeBytes);
    }

    @AfterEach
    void tearDown() throws IOException {
        // Close the current file
        WALFile currentFile = manager.getCurrentFile();
        if (currentFile != null) {
            currentFile.close();
        }
    }

    @Test
    void testConstructorAndGetters() {
        assertEquals(tempDir.toString(), manager.getDirectory());
        assertEquals(maxLogSizeBytes, manager.getMaxLogSizeBytes());
        assertNotNull(manager.getCurrentFile());
    }

    @Test
    void testCreateNewFile() throws IOException {
        // Get the current file
        WALFile oldFile = manager.getCurrentFile();

        // Create a new file
        WALFile newFile = manager.createNewFile();

        // Verify the new file is different from the old file
        assertNotEquals(oldFile.getPath(), newFile.getPath());

        // Verify the new file exists
        assertTrue(Files.exists(newFile.getPath()));
    }

    @Test
    void testFindLogFiles() throws IOException {
        // Initially, there should be one log file (created in setUp)
        List<Path> logFiles = manager.findLogFiles();
        assertEquals(1, logFiles.size());

        // Create a new file
        manager.createNewFile();

        // Now there should be two log files
        logFiles = manager.findLogFiles();
        assertEquals(2, logFiles.size());
    }

    @Test
    void testRotateLog() throws IOException {
        // Get the current file
        WALFile oldFile = manager.getCurrentFile();

        // Rotate the log
        manager.rotateLog();

        // Verify the current file is different from the old file
        assertNotEquals(oldFile.getPath(), manager.getCurrentFile().getPath());

        // Verify both files exist
        assertTrue(Files.exists(oldFile.getPath()));
        assertTrue(Files.exists(manager.getCurrentFile().getPath()));
    }

    @Test
    void testDeleteAllLogs() throws IOException {
        // Create a few log files
        manager.createNewFile();
        manager.createNewFile();

        // Verify there are multiple log files
        List<Path> logFiles = manager.findLogFiles();
        assertTrue(logFiles.size() > 1);

        // Delete all logs
        manager.deleteAllLogs();

        // Verify all old log files are deleted
        for (Path logFile : logFiles) {
            assertFalse(Files.exists(logFile));
        }

        // Verify there is a new log file
        logFiles = manager.findLogFiles();
        assertEquals(1, logFiles.size());
        assertTrue(Files.exists(logFiles.get(0)));
    }

    @Test
    void testGetNextSequenceNumber() {
        // Get the next sequence number multiple times
        long seq1 = manager.getNextSequenceNumber();
        long seq2 = manager.getNextSequenceNumber();
        long seq3 = manager.getNextSequenceNumber();

        // Verify the sequence numbers are increasing
        assertTrue(seq2 > seq1);
        assertTrue(seq3 > seq2);
    }

    // Note: The WALManagerImpl doesn't automatically rotate the log when the file size exceeds the maximum size.
    // It only checks the file size before writing and rotates the log if necessary.
    // Since we're not using the WALManagerImpl to write to the file, but directly writing to the file using the WALFile interface,
    // we can't test this functionality directly.
}
