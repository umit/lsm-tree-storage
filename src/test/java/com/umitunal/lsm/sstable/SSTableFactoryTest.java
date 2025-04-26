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

package com.umitunal.lsm.sstable;

import com.umitunal.lsm.memtable.MemTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the SSTableFactory class.
 */
class SSTableFactoryTest {

    @TempDir
    Path tempDir;

    @Test
    void testCreateFromMemTable() throws IOException {
        // Arrange
        MemTable memTable = new MemTable(1024);
        String directory = tempDir.toString();
        int level = 1;
        long sequenceNumber = 123;

        // Act
        SSTableInterface sstable = SSTableFactory.createFromMemTable(memTable, directory, level, sequenceNumber);

        // Assert
        assertNotNull(sstable);
        assertTrue(sstable instanceof SSTableImpl);
        assertEquals(level, sstable.getLevel());
        assertEquals(sequenceNumber, sstable.getSequenceNumber());

        // Clean up
        sstable.close();
    }

    @Test
    void testOpenFromDisk() throws IOException {
        // Arrange
        // First create an SSTable to open
        MemTable memTable = new MemTable(1024);
        String directory = tempDir.toString();
        int level = 1;
        long sequenceNumber = 123;
        SSTableInterface createdSstable = SSTableFactory.createFromMemTable(memTable, directory, level, sequenceNumber);
        createdSstable.close();

        // Act
        SSTableInterface openedSstable = SSTableFactory.openFromDisk(directory, level, sequenceNumber);

        // Assert
        assertNotNull(openedSstable);
        assertTrue(openedSstable instanceof SSTableImpl);
        assertEquals(level, openedSstable.getLevel());
        assertEquals(sequenceNumber, openedSstable.getSequenceNumber());

        // Clean up
        openedSstable.close();
    }

    @Test
    void testCreateBackwardCompatibleWithMemTable() throws IOException {
        // Arrange
        MemTable memTable = new MemTable(1024);
        String directory = tempDir.toString();
        int level = 1;
        long sequenceNumber = 123;

        // Act
        SSTable sstable = SSTableFactory.createBackwardCompatible(memTable, directory, level, sequenceNumber);

        // Assert
        assertNotNull(sstable);
        assertTrue(sstable instanceof SSTableAdapter);
        assertEquals(level, sstable.getLevel());
        assertEquals(sequenceNumber, sstable.getSequenceNumber());

        // Clean up
        sstable.close();
    }

    @Test
    void testCreateBackwardCompatibleWithoutMemTable() throws IOException {
        // Arrange
        // First create an SSTable to open
        MemTable memTable = new MemTable(1024);
        String directory = tempDir.toString();
        int level = 1;
        long sequenceNumber = 123;
        SSTableInterface createdSstable = SSTableFactory.createFromMemTable(memTable, directory, level, sequenceNumber);
        createdSstable.close();

        // Act
        SSTable sstable = SSTableFactory.createBackwardCompatible(null, directory, level, sequenceNumber);

        // Assert
        assertNotNull(sstable);
        assertTrue(sstable instanceof SSTableAdapter);
        assertEquals(level, sstable.getLevel());
        assertEquals(sequenceNumber, sstable.getSequenceNumber());

        // Clean up
        sstable.close();
    }
}