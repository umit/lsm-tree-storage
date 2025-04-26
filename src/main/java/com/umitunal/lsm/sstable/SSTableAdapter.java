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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Adapter class that adapts the new SSTableInterface to the old SSTable class.
 * This class ensures backward compatibility with code that uses the old SSTable class.
 */
public class SSTableAdapter extends SSTable {
    private static final Logger logger = LoggerFactory.getLogger(SSTableAdapter.class);

    private final SSTableInterface delegate;

    /**
     * Creates a new SSTableAdapter that delegates to the specified SSTableInterface.
     * 
     * @param delegate the SSTableInterface to delegate to
     * @throws IOException if an I/O error occurs
     */
    public SSTableAdapter(SSTableInterface delegate) throws IOException {
        // Call the parent constructor with a dummy directory
        super(createDummyDirectory(), 0, 0);
        this.delegate = delegate;
    }

    /**
     * Creates a dummy directory for the SSTable.
     * 
     * @return the path to the dummy directory
     * @throws RuntimeException if an I/O error occurs
     */
    private static String createDummyDirectory() {
        try {
            // Create a temporary directory for the dummy SSTable
            Path tempDir = Files.createTempDirectory("sstable-adapter");
            // Delete the temporary directory on exit
            tempDir.toFile().deleteOnExit();
            return tempDir.toString();
        } catch (IOException e) {
            throw new RuntimeException("Error creating dummy directory", e);
        }
    }

    @Override
    public byte[] get(byte[] key) {
        return delegate.get(key);
    }

    @Override
    public boolean mightContain(byte[] key) {
        return delegate.mightContain(key);
    }

    @Override
    public int getLevel() {
        return delegate.getLevel();
    }

    @Override
    public long getSequenceNumber() {
        return delegate.getSequenceNumber();
    }

    @Override
    public long getCreationTime() {
        return delegate.getCreationTime();
    }

    @Override
    public long getSizeBytes() {
        return delegate.getSizeBytes();
    }

    @Override
    public void close() {
        try {
            delegate.close();
        } catch (IOException e) {
            logger.warn("Error closing SSTable", e);
        }
    }

    @Override
    public boolean delete() {
        return delegate.delete();
    }

    @Override
    public List<byte[]> listKeys() {
        return delegate.listKeys();
    }

    @Override
    public int countEntries() {
        return delegate.countEntries();
    }

    @Override
    public Map<byte[], byte[]> getRange(byte[] startKey, byte[] endKey) {
        return delegate.getRange(startKey, endKey);
    }

    /**
     * Gets the delegate SSTableInterface.
     * 
     * @return the delegate SSTableInterface
     */
    public SSTableInterface getDelegate() {
        return delegate;
    }
}
