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

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Interface for managing WAL files.
 * This interface defines methods for creating, finding, and rotating log files.
 */
public interface WALManager {
    
    /**
     * Gets the current WAL file.
     * 
     * @return the current WAL file
     */
    WALFile getCurrentFile();
    
    /**
     * Creates a new WAL file.
     * 
     * @return the new WAL file
     * @throws IOException if an I/O error occurs
     */
    WALFile createNewFile() throws IOException;
    
    /**
     * Finds all WAL files in the directory.
     * 
     * @return a list of WAL files sorted by sequence number
     * @throws IOException if an I/O error occurs
     */
    List<Path> findLogFiles() throws IOException;
    
    /**
     * Rotates the log by creating a new log file.
     * 
     * @throws IOException if an I/O error occurs
     */
    void rotateLog() throws IOException;
    
    /**
     * Deletes all WAL files.
     * 
     * @throws IOException if an I/O error occurs
     */
    void deleteAllLogs() throws IOException;
    
    /**
     * Gets the directory where WAL files are stored.
     * 
     * @return the directory path
     */
    String getDirectory();
    
    /**
     * Gets the maximum size of a WAL file before rotation.
     * 
     * @return the maximum size in bytes
     */
    long getMaxLogSizeBytes();
    
    /**
     * Gets the next sequence number.
     * 
     * @return the next sequence number
     */
    long getNextSequenceNumber();
}