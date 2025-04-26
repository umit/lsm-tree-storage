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

package com.umitunal.lsm.sstable.iterator;

import com.umitunal.lsm.api.KeyValueIterator;
import com.umitunal.lsm.sstable.SSTableEntry;
import com.umitunal.lsm.sstable.SSTableIterator;
import com.umitunal.lsm.sstable.data.DataFileManager;
import com.umitunal.lsm.sstable.index.IndexFileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Implementation of the SSTableIteratorFactory interface.
 * This class handles the creation of iterators for SSTable files.
 */
public class SSTableIteratorFactoryImpl implements SSTableIteratorFactory {
    private static final Logger logger = LoggerFactory.getLogger(SSTableIteratorFactoryImpl.class);

    private final DataFileManager dataFileManager;
    private final IndexFileManager indexFileManager;

    /**
     * Creates a new SSTableIteratorFactoryImpl.
     * 
     * @param dataFileManager the data file manager
     * @param indexFileManager the index file manager
     */
    public SSTableIteratorFactoryImpl(DataFileManager dataFileManager, IndexFileManager indexFileManager) {
        this.dataFileManager = dataFileManager;
        this.indexFileManager = indexFileManager;
    }

    @Override
    public SSTableIterator createInMemoryIterator(SSTableEntry[] entries) {
        return new SSTableIterator.InMemoryIterator(entries);
    }

    @Override
    public SSTableIterator createFileIterator(byte[] startKey, byte[] endKey) throws IOException {
        // This is a placeholder implementation
        // The actual implementation would read entries from the file
        return new SSTableIterator.FileIterator(null, startKey, endKey);
    }

    @Override
    public KeyValueIterator createIterator(byte[] startKey, byte[] endKey) throws IOException {
        try {
            // For now, we'll use the FileIterator
            // In a more complete implementation, we might choose between different iterator types
            // based on the size of the range, the number of entries, etc.
            return createFileIterator(startKey, endKey);
        } catch (IOException e) {
            logger.error("Error creating iterator", e);
            throw e;
        }
    }
}
