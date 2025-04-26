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

package com.umitunal.lsm.wal.reader;

import com.umitunal.lsm.wal.record.Record;

import java.io.IOException;
import java.util.List;

/**
 * Interface for reading records from WAL files.
 * This interface defines methods for reading records from WAL files.
 */
public interface WALReader {
    
    /**
     * Reads all records from all WAL files.
     * 
     * @return a list of records
     * @throws IOException if an I/O error occurs
     */
    List<Record> readRecords() throws IOException;
    
    /**
     * Reads records from a specific WAL file.
     * 
     * @param filePath the path to the WAL file
     * @return a list of records
     * @throws IOException if an I/O error occurs
     */
    List<Record> readRecordsFromFile(String filePath) throws IOException;
}