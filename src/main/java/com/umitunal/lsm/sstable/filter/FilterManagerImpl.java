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

import com.umitunal.lsm.sstable.BloomFilter;
import com.umitunal.lsm.sstable.io.SSTableIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Implementation of the FilterManager interface.
 * This class handles the Bloom filter operations for SSTable files.
 */
public class FilterManagerImpl implements FilterManager {
    private static final Logger logger = LoggerFactory.getLogger(FilterManagerImpl.class);
    
    private final SSTableIO io;
    private BloomFilter filter;
    
    /**
     * Creates a new FilterManagerImpl.
     * 
     * @param io the SSTableIO instance
     */
    public FilterManagerImpl(SSTableIO io) {
        this.io = io;
        this.filter = new BloomFilter(1000, 0.01); // Default filter
    }
    
    /**
     * Creates a new FilterManagerImpl with a specified number of expected entries.
     * 
     * @param io the SSTableIO instance
     * @param expectedEntries the expected number of entries
     */
    public FilterManagerImpl(SSTableIO io, int expectedEntries) {
        this.io = io;
        this.filter = new BloomFilter(expectedEntries, 0.01);
    }
    
    @Override
    public Path getFilterFilePath() {
        return Path.of(io.getDirectory(), String.format("sst_L%d_S%d.filter", io.getLevel(), io.getSequenceNumber()));
    }
    
    @Override
    public void add(byte[] key) {
        filter.add(key);
    }
    
    @Override
    public boolean mightContain(byte[] key) {
        return filter.mightContain(key);
    }
    
    @Override
    public void save() throws IOException {
        filter.save(getFilterFilePath().toString());
    }
    
    @Override
    public void load() throws IOException {
        try {
            filter = BloomFilter.load(getFilterFilePath().toString());
        } catch (IOException e) {
            logger.warn("Error loading Bloom filter, creating a new one", e);
            filter = new BloomFilter(1000, 0.01); // Default filter
        }
    }
    
    @Override
    public void create(int expectedEntries, double falsePositiveRate) {
        filter = new BloomFilter(expectedEntries, falsePositiveRate);
    }
}