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
import com.umitunal.lsm.sstable.data.DataFileManager;
import com.umitunal.lsm.sstable.data.DataFileManagerImpl;
import com.umitunal.lsm.sstable.filter.FilterManager;
import com.umitunal.lsm.sstable.filter.FilterManagerImpl;
import com.umitunal.lsm.sstable.index.IndexFileManager;
import com.umitunal.lsm.sstable.index.IndexFileManagerImpl;
import com.umitunal.lsm.sstable.io.SSTableIO;
import com.umitunal.lsm.sstable.io.SSTableIOImpl;
import com.umitunal.lsm.sstable.iterator.SSTableIteratorFactory;
import com.umitunal.lsm.sstable.iterator.SSTableIteratorFactoryImpl;

import java.io.IOException;

/**
 * Factory for creating SSTable instances.
 * This class provides methods for creating SSTable instances with all the necessary components.
 */
public class SSTableFactory {
    
    /**
     * Creates a new SSTable from a MemTable.
     * 
     * @param memTable the MemTable to flush to disk
     * @param directory the directory to store the SSTable files
     * @param level the level of this SSTable in the LSM-tree
     * @param sequenceNumber the sequence number of this SSTable
     * @return a new SSTable instance
     * @throws IOException if an I/O error occurs
     */
    public static SSTableInterface createFromMemTable(
            MemTable memTable, 
            String directory, 
            int level, 
            long sequenceNumber) throws IOException {
        
        // Create the I/O manager
        SSTableIO io = new SSTableIOImpl(directory, level, sequenceNumber);
        
        // Create the data file manager
        DataFileManager dataFileManager = new DataFileManagerImpl(io);
        
        // Create the index file manager
        IndexFileManager indexFileManager = new IndexFileManagerImpl(io);
        
        // Create the filter manager
        FilterManager filterManager = new FilterManagerImpl(io, memTable.getEntries().size());
        
        // Create the iterator factory
        SSTableIteratorFactory iteratorFactory = new SSTableIteratorFactoryImpl(dataFileManager, indexFileManager);
        
        // Create the SSTable
        return new SSTableImpl(memTable, dataFileManager, indexFileManager, filterManager, io, iteratorFactory);
    }
    
    /**
     * Opens an existing SSTable from disk.
     * 
     * @param directory the directory containing the SSTable files
     * @param level the level of this SSTable in the LSM-tree
     * @param sequenceNumber the sequence number of this SSTable
     * @return an SSTable instance
     * @throws IOException if an I/O error occurs
     */
    public static SSTableInterface openFromDisk(
            String directory, 
            int level, 
            long sequenceNumber) throws IOException {
        
        // Create the I/O manager
        SSTableIO io = new SSTableIOImpl(directory, level, sequenceNumber);
        
        // Create the data file manager
        DataFileManager dataFileManager = new DataFileManagerImpl(io);
        
        // Create the index file manager
        IndexFileManager indexFileManager = new IndexFileManagerImpl(io);
        
        // Create the filter manager
        FilterManager filterManager = new FilterManagerImpl(io);
        
        // Create the iterator factory
        SSTableIteratorFactory iteratorFactory = new SSTableIteratorFactoryImpl(dataFileManager, indexFileManager);
        
        // Create the SSTable
        return new SSTableImpl(dataFileManager, indexFileManager, filterManager, io, iteratorFactory);
    }
    
    /**
     * Creates a backward-compatible SSTable instance.
     * This method is used to maintain compatibility with the old SSTable class.
     * 
     * @param memTable the MemTable to flush to disk, or null if opening from disk
     * @param directory the directory to store the SSTable files
     * @param level the level of this SSTable in the LSM-tree
     * @param sequenceNumber the sequence number of this SSTable
     * @return an SSTable instance that implements the old SSTable interface
     * @throws IOException if an I/O error occurs
     */
    public static SSTable createBackwardCompatible(
            MemTable memTable, 
            String directory, 
            int level, 
            long sequenceNumber) throws IOException {
        
        if (memTable != null) {
            // Create a new SSTable from a MemTable
            SSTableInterface sstable = createFromMemTable(memTable, directory, level, sequenceNumber);
            return new SSTableAdapter(sstable);
        } else {
            // Open an existing SSTable from disk
            SSTableInterface sstable = openFromDisk(directory, level, sequenceNumber);
            return new SSTableAdapter(sstable);
        }
    }
}