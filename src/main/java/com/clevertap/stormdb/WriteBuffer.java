package com.clevertap.stormdb;

import static com.clevertap.stormdb.StormDB.CRC_SIZE;
import static com.clevertap.stormdb.StormDB.KEY_SIZE;
import static com.clevertap.stormdb.StormDB.RECORDS_PER_BLOCK;
import static com.clevertap.stormdb.StormDB.RESERVED_KEY_MARKER;

import com.clevertap.stormdb.exceptions.ValueSizeTooLargeException;
import com.clevertap.stormdb.utils.RecordUtil;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.zip.CRC32;

/**
 * The {@link WriteBuffer} is a logical extension of the WAL file. For a random get, if the index
 * points to an offset greater than that of the actual WAL file, then it's assumed to be in the
 * write buffer.
 */
public class WriteBuffer {

    protected static final int FOUR_MB = 4 * 1024 * 1024;

    /**
     * Theoretically, this can go somewhere up to 15 MB, however, the corresponding buffer size will
     * be 1.5 GB. We leave this to 512 KB since it's a fairly high value.
     * <p>
     * Note: The hard limit is due to the fact that {@link ByteBuffer} accepts an int as its size.
     */
    protected static final int MAX_VALUE_SIZE = 512 * 1024;

    private ByteBuffer buffer;
    private final int valueSize;
    private final int recordSize;
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
    public WriteBuffer(final int valueSize) throws ValueSizeTooLargeException {
        this.valueSize = valueSize;
        this.recordSize = valueSize + KEY_SIZE;
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

        buffer = ByteBuffer.allocate(writeBufferSize);
    }

    public int getMaxRecords() {
        return maxRecords;
    }

    protected int getWriteBufferSize() {
        return buffer.capacity();
    }

    public int flush(final OutputStream out) throws IOException {
        if (buffer.position() == 0) {
            return 0;
        }

        // Fill the block with the last record, if required.
        while ((RecordUtil.addressToIndex(recordSize, buffer.position(), true))
                % RECORDS_PER_BLOCK != 0) {
            final int key = buffer.getInt(buffer.position() - recordSize);
            add(key, buffer.array(), buffer.position() - recordSize + KEY_SIZE);
        }

        final int bytes = buffer.position();
        out.write(buffer.array(), 0, bytes);
        out.flush();
        buffer = ByteBuffer.allocate(buffer.capacity());
        return bytes;
    }

    public byte[] array() {
        return buffer.array();
    }

    public boolean isDirty() {
        return buffer.position() > 0;
    }

    public boolean isFull() {
        return buffer.remaining() == 0; // Perfect alignment, so this works.
    }

    public int add(int key, byte[] value, int valueOffset) {
        final int address = buffer.position();
        buffer.putInt(key);
        buffer.put(value, valueOffset, valueSize);

        // Should we close this block?
        // Don't close the block if the we're adding the sync marker kv pair.
        if (key != RESERVED_KEY_MARKER) {
            final int nextRecordIndex = RecordUtil.addressToIndex(
                    recordSize, buffer.position(), true);
            if (nextRecordIndex % RECORDS_PER_BLOCK == 0) {
                closeBlock();
            }
        }
        return address;
    }

    /**
     * Always call this from a synchronised context, since it will provide a snapshot of data in the
     * current buffer.
     */
    public Enumeration<ByteBuffer> iterator() {
        final ByteBuffer ourBuffer = buffer.asReadOnlyBuffer();

        final int recordsToRead = RecordUtil.addressToIndex(recordSize, buffer.position(), true);

        return new Enumeration<ByteBuffer>() {
            int remainingRecords = recordsToRead; // -1 because we work on an index.

            @Override
            public boolean hasMoreElements() {
                return remainingRecords != 0;
            }

            @Override
            public ByteBuffer nextElement() {
                ourBuffer.position(
                        (int) RecordUtil.indexToAddress(recordSize, --remainingRecords, true));
                return ourBuffer;
            }
        };
    }

    private void closeBlock() {
        final CRC32 crc32 = new CRC32();
        final int blockSize = recordSize * RECORDS_PER_BLOCK;
        crc32.update(buffer.array(), buffer.position() - blockSize, blockSize);
        buffer.putInt((int) crc32.getValue());
        final byte[] bytes = new byte[recordSize];
        Arrays.fill(bytes, (byte) 0xFF);
        add(StormDB.RESERVED_KEY_MARKER, bytes, 0);
    }
}
