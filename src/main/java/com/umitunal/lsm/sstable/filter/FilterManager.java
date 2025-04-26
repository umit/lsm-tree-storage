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

package com.umitunal.lsm.sstable.filter;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Interface for managing Bloom filters for SSTables.
 * This interface defines methods for creating, saving, and loading Bloom filters.
 */
public interface FilterManager {
    
    /**
     * Gets the path of the filter file.
     * 
     * @return the path of the filter file
     */
    Path getFilterFilePath();
    
    /**
     * Adds a key to the Bloom filter.
     * 
     * @param key the key to add
     */
    void add(byte[] key);
    
    /**
     * Checks if a key might be in the set.
     * 
     * @param key the key to check
     * @return true if the key might be in the set, false if it definitely isn't
     */
    boolean mightContain(byte[] key);
    
    /**
     * Saves the Bloom filter to a file.
     * 
     * @throws IOException if an I/O error occurs
     */
    void save() throws IOException;
    
    /**
     * Loads the Bloom filter from a file.
     * 
     * @throws IOException if an I/O error occurs
     */
    void load() throws IOException;
    
    /**
     * Creates a new Bloom filter with the specified expected number of entries and false positive rate.
     * 
     * @param expectedEntries the expected number of entries
     * @param falsePositiveRate the desired false positive rate (e.g., 0.01 for 1%)
     */
    void create(int expectedEntries, double falsePositiveRate);
}