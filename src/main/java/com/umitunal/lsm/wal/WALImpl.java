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

package com.umitunal.lsm.wal;

import com.umitunal.lsm.wal.manager.WALManager;
import com.umitunal.lsm.wal.manager.WALManagerImpl;
import com.umitunal.lsm.wal.reader.WALReader;
import com.umitunal.lsm.wal.reader.WALReaderImpl;
import com.umitunal.lsm.wal.record.Record;
import com.umitunal.lsm.wal.writer.WALWriter;
import com.umitunal.lsm.wal.writer.WALWriterImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Implementation of the WAL interface.
 * This class coordinates the WALManager, WALReader, and WALWriter to provide the complete WAL functionality.
 */
public class WALImpl implements WAL {
    private static final Logger logger = LoggerFactory.getLogger(WALImpl.class);
    
    private final WALManager manager;
    private final WALReader reader;
    private final WALWriter writer;
    
    /**
     * Creates a new WALImpl.
     * 
     * @param directory the directory to store WAL files
     * @throws IOException if an I/O error occurs
     */
    public WALImpl(String directory) throws IOException {
        this(directory, 64 * 1024 * 1024); // Default 64MB max log size
    }
    
    /**
     * Creates a new WALImpl with a custom maximum log size.
     * 
     * @param directory the directory to store WAL files
     * @param maxLogSizeBytes maximum size of a WAL file before rotation
     * @throws IOException if an I/O error occurs
     */
    public WALImpl(String directory, long maxLogSizeBytes) throws IOException {
        this.manager = new WALManagerImpl(directory, maxLogSizeBytes);
        this.reader = new WALReaderImpl(manager);
        this.writer = new WALWriterImpl(manager);
        
        logger.info("WAL initialized in directory: " + directory);
    }
    
    @Override
    public long appendPutRecord(byte[] key, byte[] value, long ttlSeconds) throws IOException {
        return writer.appendPutRecord(key, value, ttlSeconds);
    }
    
    @Override
    public long appendDeleteRecord(byte[] key) throws IOException {
        return writer.appendDeleteRecord(key);
    }
    
    @Override
    public List<Record> readRecords() throws IOException {
        return reader.readRecords();
    }
    
    @Override
    public void deleteAllLogs() throws IOException {
        manager.deleteAllLogs();
    }
    
    @Override
    public void close() throws IOException {
        if (manager.getCurrentFile() != null) {
            manager.getCurrentFile().close();
        }
        
        logger.info("WAL closed");
    }
}