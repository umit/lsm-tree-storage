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

import com.umitunal.lsm.api.KeyValueIterator;
import com.umitunal.lsm.memtable.MemTable;
import com.umitunal.lsm.sstable.data.DataFileManager;
import com.umitunal.lsm.sstable.filter.FilterManager;
import com.umitunal.lsm.sstable.index.IndexFileManager;
import com.umitunal.lsm.sstable.io.SSTableIO;
import com.umitunal.lsm.sstable.iterator.SSTableIteratorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the SSTableInterface that delegates to the appropriate components.
 * This class coordinates the different components of the SSTable.
 */
public class SSTableImpl implements SSTableInterface {
    private static final Logger logger = LoggerFactory.getLogger(SSTableImpl.class);

    private final DataFileManager dataFileManager;
    private final IndexFileManager indexFileManager;
    private final FilterManager filterManager;
    private final SSTableIO io;
    private final SSTableIteratorFactory iteratorFactory;

    private final int level;
    private final long sequenceNumber;
    private final long creationTime;

    /**
     * Creates a new SSTable from a MemTable.
     * 
     * @param memTable the MemTable to flush to disk
     * @param dataFileManager the data file manager
     * @param indexFileManager the index file manager
     * @param filterManager the filter manager
     * @param io the I/O manager
     * @param iteratorFactory the iterator factory
     * @throws IOException if an I/O error occurs
     */
    public SSTableImpl(
            MemTable memTable,
            DataFileManager dataFileManager,
            IndexFileManager indexFileManager,
            FilterManager filterManager,
            SSTableIO io,
            SSTableIteratorFactory iteratorFactory) throws IOException {

        this.dataFileManager = dataFileManager;
        this.indexFileManager = indexFileManager;
        this.filterManager = filterManager;
        this.io = io;
        this.iteratorFactory = iteratorFactory;

        this.level = io.getLevel();
        this.sequenceNumber = io.getSequenceNumber();
        this.creationTime = System.currentTimeMillis();

        // Flush the MemTable to disk
        io.flushToDisk(memTable);

        logger.info("Created SSTable: level=" + level + ", sequenceNumber=" + sequenceNumber);
    }

    /**
     * Opens an existing SSTable from disk.
     * 
     * @param dataFileManager the data file manager
     * @param indexFileManager the index file manager
     * @param filterManager the filter manager
     * @param io the I/O manager
     * @param iteratorFactory the iterator factory
     * @throws IOException if an I/O error occurs
     */
    public SSTableImpl(
            DataFileManager dataFileManager,
            IndexFileManager indexFileManager,
            FilterManager filterManager,
            SSTableIO io,
            SSTableIteratorFactory iteratorFactory) throws IOException {

        this.dataFileManager = dataFileManager;
        this.indexFileManager = indexFileManager;
        this.filterManager = filterManager;
        this.io = io;
        this.iteratorFactory = iteratorFactory;

        this.level = io.getLevel();
        this.sequenceNumber = io.getSequenceNumber();
        this.creationTime = System.currentTimeMillis();

        // Load the SSTable from disk
        io.loadFromDisk();

        logger.info("Opened SSTable: level=" + level + ", sequenceNumber=" + sequenceNumber);
    }

    @Override
    public byte[] get(byte[] key) {
        if (key == null || key.length == 0) {
            return null;
        }

        // First check the bloom filter for a quick negative
        if (!mightContain(key)) {
            return null; // Definitely not in the set
        }

        try {
            // Find the closest key in the sparse index
            var indexEntry = indexFileManager.findClosestKey(key);

            if (indexEntry == null) {
                // No entry in the sparse index that is less than or equal to the key
                // Start from the beginning of the data file (after the header)
                return dataFileManager.findKeyInDataFile(key, 16);
            } else {
                // Start searching from the position in the sparse index
                return dataFileManager.findKeyInDataFile(key, indexEntry.getValue());
            }
        } catch (IOException e) {
            logger.error("Error reading from SSTable", e);
            return null;
        }
    }

    @Override
    public boolean mightContain(byte[] key) {
        return filterManager.mightContain(key);
    }

    @Override
    public int getLevel() {
        return level;
    }

    @Override
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getSizeBytes() {
        try {
            long size = 0;

            // Add the size of the data file
            size += dataFileManager.getDataChannel().size();

            // Add the size of the index file
            size += indexFileManager.getIndexChannel().size();

            return size;
        } catch (IOException e) {
            logger.warn("Error getting SSTable size", e);
            return 0;
        }
    }

    @Override
    public boolean delete() {
        try {
            // Close first to release resources
            close();

            // Delete the files
            return io.deleteFiles();
        } catch (IOException e) {
            logger.warn("Error deleting SSTable files", e);
            return false;
        }
    }

    @Override
    public List<byte[]> listKeys() {
        try {
            return dataFileManager.listKeys();
        } catch (IOException e) {
            logger.error("Error listing keys from SSTable", e);
            return List.of();
        }
    }

    @Override
    public int countEntries() {
        try {
            return dataFileManager.countEntries();
        } catch (IOException e) {
            logger.error("Error counting entries in SSTable", e);
            return 0;
        }
    }

    @Override
    public Map<byte[], byte[]> getRange(byte[] startKey, byte[] endKey) {
        try {
            return dataFileManager.getRange(startKey, endKey);
        } catch (IOException e) {
            logger.error("Error getting range from SSTable", e);
            return Map.of();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            dataFileManager.close();
            indexFileManager.close();
            io.close();
        } catch (IOException e) {
            logger.warn("Error closing SSTable resources", e);
            throw e;
        }
    }

    /**
     * Creates an iterator for the specified range.
     * 
     * @param startKey the start key (inclusive), or null for the first key
     * @param endKey the end key (exclusive), or null for no upper bound
     * @return an iterator for the specified range
     * @throws IOException if an I/O error occurs
     */
    public KeyValueIterator getIterator(byte[] startKey, byte[] endKey) throws IOException {
        return iteratorFactory.createIterator(startKey, endKey);
    }
}
