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

package com.umitunal.lsm.wal.writer;

import java.io.IOException;

/**
 * Interface for writing records to WAL files.
 * This interface defines methods for writing records to WAL files.
 */
public interface WALWriter {
    
    /**
     * Appends a put record to the WAL.
     * 
     * @param key the key as byte array
     * @param value the value as byte array
     * @param ttlSeconds time-to-live in seconds, 0 means no expiration
     * @return the sequence number of the record
     * @throws IOException if an I/O error occurs
     */
    long appendPutRecord(byte[] key, byte[] value, long ttlSeconds) throws IOException;
    
    /**
     * Appends a delete record to the WAL.
     * 
     * @param key the key as byte array
     * @return the sequence number of the record
     * @throws IOException if an I/O error occurs
     */
    long appendDeleteRecord(byte[] key) throws IOException;
}