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

package com.umitunal.lsm.sstable.data;

import com.umitunal.lsm.sstable.SSTableEntry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Interface for managing SSTable data files.
 * This interface defines methods for reading and writing data to the data file.
 */
public interface DataFileManager extends AutoCloseable {
    
    /**
     * Gets the path of the data file.
     * 
     * @return the path of the data file
     */
    Path getDataFilePath();
    
    /**
     * Gets the file channel for the data file.
     * 
     * @return the file channel
     */
    FileChannel getDataChannel();
    
    /**
     * Writes an entry to the data file.
     * 
     * @param entry the entry to write
     * @return the offset in the file where the entry was written
     * @throws IOException if an I/O error occurs
     */
    long writeEntry(SSTableEntry entry) throws IOException;
    
    /**
     * Reads an entry from the data file at the specified offset.
     * 
     * @param offset the offset in the file
     * @return the entry, or null if the offset is invalid
     * @throws IOException if an I/O error occurs
     */
    SSTableEntry readEntry(long offset) throws IOException;
    
    /**
     * Finds a key in the data file starting from the given position.
     * 
     * @param key the key to find
     * @param startPosition the position to start searching from
     * @return the value as byte array, or null if the key doesn't exist
     * @throws IOException if an I/O error occurs
     */
    byte[] findKeyInDataFile(byte[] key, long startPosition) throws IOException;
    
    /**
     * Gets all key-value pairs with keys in the specified range.
     * The range is inclusive of startKey and exclusive of endKey.
     * 
     * @param startKey the start of the range (inclusive), or null for the first key
     * @param endKey the end of the range (exclusive), or null for no upper bound
     * @return a map of keys to values in the specified range
     * @throws IOException if an I/O error occurs
     */
    Map<byte[], byte[]> getRange(byte[] startKey, byte[] endKey) throws IOException;
    
    /**
     * Lists all keys in the data file.
     * 
     * @return a list of all keys in the data file, excluding tombstones
     * @throws IOException if an I/O error occurs
     */
    List<byte[]> listKeys() throws IOException;
    
    /**
     * Counts the number of entries in the data file.
     * 
     * @return the number of entries in the data file
     * @throws IOException if an I/O error occurs
     */
    int countEntries() throws IOException;
    
    /**
     * Closes the data file and releases resources.
     * 
     * @throws IOException if an I/O error occurs
     */
    @Override
    void close() throws IOException;
}