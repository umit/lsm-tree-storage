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

import java.io.IOException;

/**
 * Factory interface for creating SSTable iterators.
 * This interface defines methods for creating different types of iterators.
 */
public interface SSTableIteratorFactory {

    /**
     * Creates an in-memory iterator for the specified entries.
     * 
     * @param entries the entries to iterate over
     * @return an in-memory iterator
     */
    SSTableIterator createInMemoryIterator(SSTableEntry[] entries);

    /**
     * Creates a file iterator for the specified range.
     * 
     * @param startKey the start key (inclusive), or null for the first key
     * @param endKey the end key (exclusive), or null for no upper bound
     * @return a file iterator
     * @throws IOException if an I/O error occurs
     */
    SSTableIterator createFileIterator(byte[] startKey, byte[] endKey) throws IOException;

    /**
     * Creates a key-value iterator for the specified range.
     * This is a convenience method that returns a KeyValueIterator instead of an SSTableIterator.
     * 
     * @param startKey the start key (inclusive), or null for the first key
     * @param endKey the end key (exclusive), or null for no upper bound
     * @return a key-value iterator
     * @throws IOException if an I/O error occurs
     */
    KeyValueIterator createIterator(byte[] startKey, byte[] endKey) throws IOException;
}
