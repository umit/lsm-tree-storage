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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Implementation of the WALFile interface.
 * This class handles the actual file operations for the WAL.
 */
public class WALFileImpl implements WALFile {
    private static final Logger logger = LoggerFactory.getLogger(WALFileImpl.class);

    private final Path path;
    private final long sequenceNumber;
    private final FileChannel channel;

    /**
     * Creates a new WALFileImpl for an existing file.
     * 
     * @param path the path to the WAL file
     * @param sequenceNumber the sequence number of this WAL file
     * @param options the options for opening the file
     * @throws IOException if an I/O error occurs
     */
    public WALFileImpl(Path path, long sequenceNumber, StandardOpenOption... options) throws IOException {
        this.path = path;
        this.sequenceNumber = sequenceNumber;
        this.channel = FileChannel.open(path, options);
        logger.debug("Opened WAL file: " + path);
    }

    @Override
    public Path getPath() {
        return path;
    }

    @Override
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public int write(ByteBuffer buffer) throws IOException {
        return channel.write(buffer);
    }

    @Override
    public int read(ByteBuffer buffer, long position) throws IOException {
        return channel.read(buffer, position);
    }

    @Override
    public void force(boolean metaData) throws IOException {
        channel.force(metaData);
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public FileChannel getChannel() {
        return channel;
    }

    @Override
    public void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
            logger.debug("Closed WAL file: " + path);
        }
    }

    @Override
    public String toString() {
        return "WALFile{" +
                "path=" + path +
                ", sequenceNumber=" + sequenceNumber +
                '}';
    }
}
