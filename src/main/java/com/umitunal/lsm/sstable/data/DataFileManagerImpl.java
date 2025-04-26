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


import com.umitunal.lsm.api.ByteArrayWrapper;
import com.umitunal.lsm.sstable.SSTableEntry;
import com.umitunal.lsm.sstable.io.SSTableIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;

/**
 * Implementation of the DataFileManager interface.
 * This class handles the data file operations for SSTable files.
 */
public class DataFileManagerImpl implements DataFileManager {
    private static final Logger logger = LoggerFactory.getLogger(DataFileManagerImpl.class);

    private final SSTableIO io;

    /**
     * Creates a new DataFileManagerImpl.
     *
     * @param io the SSTableIO instance
     */
    public DataFileManagerImpl(SSTableIO io) {
        this.io = io;
    }

    @Override
    public Path getDataFilePath() {
        return Path.of(io.getDirectory(), String.format("sst_L%d_S%d.data", io.getLevel(), io.getSequenceNumber()));
    }

    @Override
    public FileChannel getDataChannel() {
        return io.getDataChannel();
    }

    @Override
    public long writeEntry(SSTableEntry entry) throws IOException {
        ByteBuffer buffer = io.writeEntryHeader(entry);
        return getDataChannel().write(buffer);
    }

    @Override
    public SSTableEntry readEntry(long offset) throws IOException {
        FileChannel channel = getDataChannel();

        // Read key length
        ByteBuffer keyLengthBuffer = ByteBuffer.allocate(4);
        channel.read(keyLengthBuffer, offset);
        keyLengthBuffer.flip();
        int keyLength = keyLengthBuffer.getInt();

        // Read key
        ByteBuffer keyBuffer = ByteBuffer.allocate(keyLength);
        channel.read(keyBuffer, offset + 4);
        keyBuffer.flip();
        byte[] key = new byte[keyLength];
        keyBuffer.get(key);

        // Read value length
        ByteBuffer valueLengthBuffer = ByteBuffer.allocate(4);
        channel.read(valueLengthBuffer, offset + 4 + keyLength);
        valueLengthBuffer.flip();
        int valueLength = valueLengthBuffer.getInt();

        // Check if this is a tombstone
        if (valueLength == 0) {
            // Read timestamp
            ByteBuffer timestampBuffer = ByteBuffer.allocate(8);
            channel.read(timestampBuffer, offset + 4 + keyLength + 4);
            timestampBuffer.flip();
            long timestamp = timestampBuffer.getLong();

            return SSTableEntry.tombstone(key, timestamp);
        } else {
            // Read value
            ByteBuffer valueBuffer = ByteBuffer.allocate(valueLength);
            channel.read(valueBuffer, offset + 4 + keyLength + 4);
            valueBuffer.flip();
            byte[] value = new byte[valueLength];
            valueBuffer.get(value);

            // Read timestamp
            ByteBuffer timestampBuffer = ByteBuffer.allocate(8);
            channel.read(timestampBuffer, offset + 4 + keyLength + 4 + valueLength);
            timestampBuffer.flip();
            long timestamp = timestampBuffer.getLong();

            return SSTableEntry.of(key, value, timestamp);
        }
    }

    @Override
    public byte[] findKeyInDataFile(byte[] key, long startPosition) throws IOException {
        FileChannel channel = getDataChannel();
        ByteBuffer buffer = ByteBuffer.allocate(1024); // Initial buffer size
        long position = startPosition;

        while (position < channel.size()) {
            // Read key length
            buffer.clear();
            buffer.limit(4);
            channel.read(buffer, position);
            buffer.flip();
            int keyLength = buffer.getInt();
            position += 4;

            // Read key
            buffer.clear();
            buffer.limit(keyLength);
            if (buffer.capacity() < keyLength) {
                buffer = ByteBuffer.allocate(keyLength);
            }
            channel.read(buffer, position);
            buffer.flip();
            byte[] entryKey = new byte[keyLength];
            buffer.get(entryKey);
            position += keyLength;

            // Read value length
            buffer.clear();
            buffer.limit(4);
            channel.read(buffer, position);
            buffer.flip();
            int valueLength = buffer.getInt();
            position += 4;

            // Check if this is a tombstone
            if (valueLength == 0) {
                // Skip the timestamp (8 bytes)
                position += 8;

                // If this is the key we're looking for, it's been deleted
                if (Arrays.equals(key, entryKey)) {
                    return null;
                }

                continue;
            }

            // Read value
            buffer.clear();
            buffer.limit(valueLength);
            if (buffer.capacity() < valueLength) {
                buffer = ByteBuffer.allocate(valueLength);
            }
            channel.read(buffer, position);
            buffer.flip();
            byte[] value = new byte[valueLength];
            buffer.get(value);
            position += valueLength;

            // Skip the timestamp (8 bytes)
            position += 8;

            // If this is the key we're looking for, return the value
            if (Arrays.equals(key, entryKey)) {
                return value;
            }
        }

        // Key not found
        return null;
    }

    @Override
    public Map<byte[], byte[]> getRange(byte[] startKey, byte[] endKey) throws IOException {
        Map<byte[], byte[]> result = new TreeMap<>((a, b) -> {
            ByteArrayWrapper wrapperA = new ByteArrayWrapper(a);
            ByteArrayWrapper wrapperB = new ByteArrayWrapper(b);
            return wrapperA.compareTo(wrapperB);
        });

        FileChannel channel = getDataChannel();
        ByteBuffer buffer = ByteBuffer.allocate(1024); // Initial buffer size
        long position = 16; // Start after the header

        while (position < channel.size()) {
            // Read key length
            buffer.clear();
            buffer.limit(4);
            channel.read(buffer, position);
            buffer.flip();
            int keyLength = buffer.getInt();
            position += 4;

            // Read key
            buffer.clear();
            buffer.limit(keyLength);
            if (buffer.capacity() < keyLength) {
                buffer = ByteBuffer.allocate(keyLength);
            }
            channel.read(buffer, position);
            buffer.flip();
            byte[] key = new byte[keyLength];
            buffer.get(key);
            position += keyLength;

            // Check if the key is in the range
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
            boolean inRange = true;
            if (startKey != null && keyWrapper.compareTo(new ByteArrayWrapper(startKey)) < 0) {
                inRange = false; // Key is before the start of the range
            }
            if (endKey != null && keyWrapper.compareTo(new ByteArrayWrapper(endKey)) >= 0) {
                inRange = false; // Key is at or after the end of the range
            }

            // Read value length
            buffer.clear();
            buffer.limit(4);
            channel.read(buffer, position);
            buffer.flip();
            int valueLength = buffer.getInt();
            position += 4;

            // Skip tombstones
            if (valueLength == 0) {
                // Skip the timestamp (8 bytes)
                position += 8;
                continue;
            }

            // If the key is in the range, add it to the result
            if (inRange) {
                // Read value
                buffer.clear();
                buffer.limit(valueLength);
                if (buffer.capacity() < valueLength) {
                    buffer = ByteBuffer.allocate(valueLength);
                }
                channel.read(buffer, position);
                buffer.flip();
                byte[] value = new byte[valueLength];
                buffer.get(value);

                result.put(key, value);
            }

            // Skip the value (if we didn't read it) and timestamp
            position += valueLength + 8;
        }

        return result;
    }

    @Override
    public List<byte[]> listKeys() throws IOException {
        List<byte[]> keys = new ArrayList<>();

        FileChannel channel = getDataChannel();
        ByteBuffer buffer = ByteBuffer.allocate(1024); // Initial buffer size
        long position = 16; // Start after the header

        while (position < channel.size()) {
            // Read key length
            buffer.clear();
            buffer.limit(4);
            channel.read(buffer, position);
            buffer.flip();
            int keyLength = buffer.getInt();
            position += 4;

            // Read key
            buffer.clear();
            buffer.limit(keyLength);
            if (buffer.capacity() < keyLength) {
                buffer = ByteBuffer.allocate(keyLength);
            }
            channel.read(buffer, position);
            buffer.flip();
            byte[] key = new byte[keyLength];
            buffer.get(key);
            position += keyLength;

            // Read value length
            buffer.clear();
            buffer.limit(4);
            channel.read(buffer, position);
            buffer.flip();
            int valueLength = buffer.getInt();
            position += 4;

            // Skip tombstones
            if (valueLength == 0) {
                // Skip the timestamp (8 bytes)
                position += 8;
                continue;
            }

            // Add the key to the list
            keys.add(key);

            // Skip the value and timestamp
            position += valueLength + 8;
        }

        return keys;
    }

    @Override
    public int countEntries() throws IOException {
        int count = 0;

        FileChannel channel = getDataChannel();
        ByteBuffer buffer = ByteBuffer.allocate(1024); // Initial buffer size
        long position = 16; // Start after the header

        while (position < channel.size()) {
            // Read key length
            buffer.clear();
            buffer.limit(4);
            channel.read(buffer, position);
            buffer.flip();
            int keyLength = buffer.getInt();
            position += 4;

            // Skip key
            position += keyLength;

            // Read value length
            buffer.clear();
            buffer.limit(4);
            channel.read(buffer, position);
            buffer.flip();
            int valueLength = buffer.getInt();
            position += 4;

            // Skip tombstones
            if (valueLength == 0) {
                // Skip the timestamp (8 bytes)
                position += 8;
                continue;
            }

            // Increment count
            count++;

            // Skip the value and timestamp
            position += valueLength + 8;
        }

        return count;
    }

    @Override
    public void close() throws IOException {
        // The SSTableIO will close the data channel
    }
}