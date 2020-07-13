package com.clevertap.stormdb;

import static com.clevertap.stormdb.StormDB.CRC_SIZE;
import static com.clevertap.stormdb.StormDB.KEY_SIZE;
import static com.clevertap.stormdb.StormDB.RECORDS_PER_BLOCK;
import static com.clevertap.stormdb.StormDB.RESERVED_KEY_MARKER;

import com.clevertap.stormdb.exceptions.StormDBRuntimeException;
import com.clevertap.stormdb.exceptions.ValueSizeTooLargeException;
import com.clevertap.stormdb.utils.RecordUtil;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.CRC32;

/**
 * The {@link Buffer} is a logical extension of the WAL file. For a random get, if the index
 * points to an offset greater than that of the actual WAL file, then it's assumed to be in the
 * write buffer.
 */
public class Buffer {

    protected static final int FOUR_MB = 4 * 1024 * 1024;

    /**
     * Theoretically, this can go somewhere up to 15 MB, however, the corresponding buffer size will
     * be 1.5 GB. We leave this to 512 KB since it's a fairly high value.
     * <p>
     * Note: The hard limit is due to the fact that {@link ByteBuffer} accepts an int as its size.
     */
    protected static final int MAX_VALUE_SIZE = 512 * 1024;

    private ByteBuffer byteBuffer;
    private final int valueSize;
    private final int recordSize;
    private final boolean readOnly;
    private final boolean wal;
    private final int maxRecords;

    /**
     * Initialises a write buffer for the WAL file with the following specification:
     * <ol>
     *     <li>Calculates how many records can fit within a 4 MB buffer</li>
     *     <li>If it turns out to be less than {@link StormDB#RECORDS_PER_BLOCK}, it chooses 128
     *     (this will happen for very large values)</li>
     *     <li>Now, make this a multiple of 128</li>
     *     <li>Then calculate how many CRCs and sync markers need to be accommodated</li>
     *     <li>Finally, initialise a write buffer of the sum of bytes required</li>
     * </ol>
     *
     * @param valueSize The size of each value in this database
     */
    public Buffer(final int valueSize, final boolean readOnly, final boolean wal) {
        this.valueSize = valueSize;
        this.recordSize = valueSize + KEY_SIZE;
        this.readOnly = readOnly;
        this.wal = wal;
        if (valueSize > MAX_VALUE_SIZE) {
            throw new ValueSizeTooLargeException();
        }

        int recordsToBuffer = Math.max(FOUR_MB / recordSize, RECORDS_PER_BLOCK);

        // Get to the nearest multiple of 128.
        recordsToBuffer = (recordsToBuffer / RECORDS_PER_BLOCK) * RECORDS_PER_BLOCK;
        this.maxRecords = recordsToBuffer;

        final int blocks = recordsToBuffer / RECORDS_PER_BLOCK;

        // Each block will have 1 CRC and 1 sync marker (the sync marker is one kv pair)
        final int writeBufferSize = blocks * RECORDS_PER_BLOCK * recordSize
                + (blocks * (CRC_SIZE + recordSize));

        byteBuffer = ByteBuffer.allocate(writeBufferSize);
    }

    public int getMaxRecords() {
        return maxRecords;
    }

    protected int getWriteBufferSize() {
        return byteBuffer.capacity();
    }

    public int flush(final OutputStream out) throws IOException {
        if (readOnly) {
            throw new StormDBRuntimeException("Initialised in read only mode!");
        }

        if (byteBuffer.position() == 0) {
            return 0;
        }

        // Fill the block with the last record, if required.
        while ((RecordUtil.addressToIndex(recordSize, byteBuffer.position(), wal))
                % RECORDS_PER_BLOCK != 0) {
            final int key = byteBuffer.getInt(byteBuffer.position() - recordSize);
            add(key, byteBuffer.array(), byteBuffer.position() - recordSize + KEY_SIZE);
        }

        final int bytes = byteBuffer.position();
        out.write(byteBuffer.array(), 0, bytes);
        out.flush();
        return bytes;
    }

    public void readFromFiles(List<RandomAccessFile> files,
            final Consumer<ByteBuffer> recordConsumer) throws IOException {
        for (RandomAccessFile file : files) {
            readFromFile(file, recordConsumer);
        }
    }

    public void readFromFile(final RandomAccessFile file, final Consumer<ByteBuffer> recordConsumer)
            throws IOException {
        final int blockSize = RecordUtil.blockSizeWithTrailer(recordSize);

        if (wal) {
            while (file.getFilePointer() != 0) {
                byteBuffer.clear();
                final long validBytesRemaining = file.getFilePointer() - blockSize;
                file.seek(Math.max(validBytesRemaining, 0));

                fillBuffer(file, recordConsumer);

                // Set the position again, since the read op moved the cursor ahead.
                file.seek(Math.max(validBytesRemaining, 0));
            }
        } else {
            while (true) {
                byteBuffer.clear();
                final int bytesRead = fillBuffer(file, recordConsumer);
                if (bytesRead < blockSize) {
                    break;
                }
            }
        }
        // TODO: 13/07/2020 we cannot use a threadlocal, since in a multi db scenario, we'll keep opening files - use a hashmap instead
    }

    private int fillBuffer(RandomAccessFile file, Consumer<ByteBuffer> recordConsumer)
            throws IOException {
        final int bytesRead = file.read(byteBuffer.array());
        if (bytesRead == -1) { // No more data.
            return 0;
        }
        byteBuffer.position(bytesRead);
        byteBuffer.limit(bytesRead);

        // Note: There's the possibility that we'll read the head of the file twice,
        // but that's okay, since we iterate in a backwards fashion.
        final Enumeration<ByteBuffer> iterator = iterator();
        while (iterator.hasMoreElements()) {
            recordConsumer.accept(iterator.nextElement());
        }

        return bytesRead;
    }

    public byte[] array() {
        return byteBuffer.array();
    }

    public boolean isDirty() {
        return byteBuffer.position() > 0;
    }

    public boolean isFull() {
        return byteBuffer.remaining() == 0; // Perfect alignment, so this works.
    }

    public int add(int key, byte[] value, int valueOffset) {
        if (readOnly) {
            throw new StormDBRuntimeException("Initialised in read only mode!");
        }
        final int address = byteBuffer.position();

        if (!wal
                && (RecordUtil.addressToIndex(recordSize, address, wal) % RECORDS_PER_BLOCK) + 1
                == 0) {
            insertSyncMarker();
        }

        byteBuffer.putInt(key);
        byteBuffer.put(value, valueOffset, valueSize);

        // Should we close this block?
        // Don't close the block if the we're adding the sync marker kv pair.
        final int nextRecordIndex = RecordUtil.addressToIndex(
                recordSize, byteBuffer.position(), wal);
        if (nextRecordIndex % RECORDS_PER_BLOCK == 0) {
            closeBlock();
        }
        return address;
    }

    /**
     * Always call this from a synchronised context, since it will provide a snapshot of data in the
     * current buffer.
     */
    public Enumeration<ByteBuffer> iterator() {
        final ByteBuffer ourBuffer = byteBuffer.duplicate();

        final int recordsToRead = RecordUtil.addressToIndex(recordSize, byteBuffer.position(), wal);

        return new Enumeration<ByteBuffer>() {
            int currentRecordIndex = wal ? recordsToRead : 0;

            @Override
            public boolean hasMoreElements() {
                if (wal) {
                    return currentRecordIndex != 0;
                } else {
                    return currentRecordIndex < recordsToRead;
                }
            }

            @Override
            public ByteBuffer nextElement() {
                final int position;
                if (wal) {
                    position = (int) RecordUtil
                            .indexToAddress(recordSize, --currentRecordIndex, true);
                } else {
                    position = (int) RecordUtil
                            .indexToAddress(recordSize, currentRecordIndex++, false);
                }
                ourBuffer.position(position);
                return ourBuffer;
            }
        };
    }

    private void closeBlock() {
        final CRC32 crc32 = new CRC32();
        final int blockSize = recordSize * RECORDS_PER_BLOCK;
        crc32.update(byteBuffer.array(), byteBuffer.position() - blockSize, blockSize);
        byteBuffer.putInt((int) crc32.getValue());
        if (wal) {
            insertSyncMarker();
        }
    }

    protected void insertSyncMarker() {
        final byte[] bytes = new byte[valueSize];
        Arrays.fill(bytes, (byte) 0xFF);
        byteBuffer.putInt(RESERVED_KEY_MARKER);
        byteBuffer.put(bytes);
    }

    public void clear() {
        byteBuffer = ByteBuffer.allocate(byteBuffer.capacity());
    }
}
