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

package com.umitunal.lsm.wal.file;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the WALFileImpl class.
 */
class WALFileImplTest {

    @TempDir
    Path tempDir;

    private Path walFilePath;
    private WALFileImpl walFile;
    private final long sequenceNumber = 123;

    @BeforeEach
    void setUp() throws IOException {
        walFilePath = tempDir.resolve("test_wal.log");
        walFile = new WALFileImpl(walFilePath, sequenceNumber, 
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (walFile != null) {
            walFile.close();
        }
    }

    @Test
    void testConstructorAndGetters() {
        assertEquals(walFilePath, walFile.getPath());
        assertEquals(sequenceNumber, walFile.getSequenceNumber());
        assertNotNull(walFile.getChannel());
        assertTrue(walFile.getChannel().isOpen());
    }

    @Test
    void testWriteAndRead() throws IOException {
        // Write data to the file
        ByteBuffer writeBuffer = ByteBuffer.wrap("test data".getBytes());
        int bytesWritten = walFile.write(writeBuffer);
        assertEquals(9, bytesWritten); // "test data" is 9 bytes

        // Read data from the file
        ByteBuffer readBuffer = ByteBuffer.allocate(9);
        int bytesRead = walFile.read(readBuffer, 0);
        assertEquals(9, bytesRead);

        // Verify the data
        readBuffer.flip();
        byte[] data = new byte[9];
        readBuffer.get(data);
        assertEquals("test data", new String(data));
    }

    @Test
    void testForce() throws IOException {
        // Write data to the file
        ByteBuffer writeBuffer = ByteBuffer.wrap("test data".getBytes());
        walFile.write(writeBuffer);

        // Force the data to disk
        assertDoesNotThrow(() -> walFile.force(true));
    }

    @Test
    void testSize() throws IOException {
        // Initially, the file should be empty
        assertEquals(0, walFile.size());

        // Write data to the file
        ByteBuffer writeBuffer = ByteBuffer.wrap("test data".getBytes());
        walFile.write(writeBuffer);

        // Check the size
        assertEquals(9, walFile.size()); // "test data" is 9 bytes
    }

    @Test
    void testClose() throws IOException {
        // Close the file
        walFile.close();

        // Verify the channel is closed
        assertFalse(walFile.getChannel().isOpen());

        // Closing again should not throw an exception
        assertDoesNotThrow(() -> walFile.close());
    }

    @Test
    void testToString() {
        String toString = walFile.toString();
        assertTrue(toString.contains("path=" + walFilePath));
        assertTrue(toString.contains("sequenceNumber=" + sequenceNumber));
    }
}