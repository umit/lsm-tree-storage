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

package com.umitunal.lsm.sstable.io;


import com.umitunal.lsm.memtable.MemTable;
import com.umitunal.lsm.sstable.SSTableEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Interface for SSTable I/O operations.
 * This interface defines methods for reading and writing SSTable files.
 */
public interface SSTableIO extends AutoCloseable {

    /**
     * Gets the directory where SSTable files are stored.
     *
     * @return the directory path
     */
    String getDirectory();

    /**
     * Gets the level of this SSTable in the LSM-tree.
     *
     * @return the level
     */
    int getLevel();

    /**
     * Gets the sequence number of this SSTable.
     *
     * @return the sequence number
     */
    long getSequenceNumber();

    /**
     * Gets the data file channel.
     *
     * @return the data file channel
     */
    FileChannel getDataChannel();

    /**
     * Gets the index file channel.
     *
     * @return the index file channel
     */
    FileChannel getIndexChannel();

    /**
     * Flushes a MemTable to disk as an SSTable.
     *
     * @param memTable the MemTable to flush
     * @throws IOException if an I/O error occurs
     */
    void flushToDisk(MemTable memTable) throws IOException;

    /**
     * Loads an SSTable from disk.
     *
     * @throws IOException if an I/O error occurs
     */
    void loadFromDisk() throws IOException;

    /**
     * Writes an entry header to a buffer.
     *
     * @param entry the entry to write
     * @return the buffer containing the header
     */
    ByteBuffer writeEntryHeader(SSTableEntry entry);

    /**
     * Reads an entry header from a buffer.
     *
     * @param buffer the buffer containing the header
     * @return the entry header information (key length, value length, timestamp, tombstone)
     */
    EntryHeader readEntryHeader(ByteBuffer buffer);

    /**
     * Deletes all files associated with this SSTable.
     *
     * @return true if all files were deleted successfully, false otherwise
     */
    boolean deleteFiles();

    /**
     * Closes all file channels and releases resources.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    void close() throws IOException;

    /**
     * Record for entry header information.
     */
    record EntryHeader(int keyLength, int valueLength, long timestamp, boolean tombstone) {
    }
}
