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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * Interface for WAL file operations.
 * This interface defines methods for working with WAL files.
 */
public interface WALFile extends AutoCloseable {
    
    /**
     * Gets the path of this WAL file.
     * 
     * @return the path
     */
    Path getPath();
    
    /**
     * Gets the sequence number of this WAL file.
     * 
     * @return the sequence number
     */
    long getSequenceNumber();
    
    /**
     * Writes data to the WAL file.
     * 
     * @param buffer the buffer containing the data to write
     * @return the number of bytes written
     * @throws IOException if an I/O error occurs
     */
    int write(ByteBuffer buffer) throws IOException;
    
    /**
     * Reads data from the WAL file.
     * 
     * @param buffer the buffer to read into
     * @param position the position in the file to read from
     * @return the number of bytes read
     * @throws IOException if an I/O error occurs
     */
    int read(ByteBuffer buffer, long position) throws IOException;
    
    /**
     * Forces any updates to this file to be written to the storage device.
     * 
     * @param metaData whether to force updates to metadata to be written
     * @throws IOException if an I/O error occurs
     */
    void force(boolean metaData) throws IOException;
    
    /**
     * Gets the size of this WAL file.
     * 
     * @return the size in bytes
     * @throws IOException if an I/O error occurs
     */
    long size() throws IOException;
    
    /**
     * Gets the file channel for this WAL file.
     * 
     * @return the file channel
     */
    FileChannel getChannel();
    
    /**
     * Closes this WAL file.
     * 
     * @throws IOException if an I/O error occurs
     */
    @Override
    void close() throws IOException;
}