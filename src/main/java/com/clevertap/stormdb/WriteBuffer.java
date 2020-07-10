package com.clevertap.stormdb;

import com.clevertap.stormdb.exceptions.ValueSizeTooLargeException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

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

    private final ByteBuffer buffer;

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
        if (valueSize > MAX_VALUE_SIZE) {
            throw new ValueSizeTooLargeException();
        }

        final int recordSize = valueSize + 4; // Keys are always 4 bytes.
        int recordsToBuffer = Math.max(FOUR_MB / recordSize, StormDB.RECORDS_PER_BLOCK);

        // Get to the nearest multiple of 128.
        recordsToBuffer = (recordsToBuffer / StormDB.RECORDS_PER_BLOCK) * StormDB.RECORDS_PER_BLOCK;

        final int blocks = recordsToBuffer / StormDB.RECORDS_PER_BLOCK;
        final int crcSize = 4;
        // Each block will have 1 CRC and 1 sync marker (the sync marker is one kv pair)
        final int writeBufferSize = blocks * StormDB.RECORDS_PER_BLOCK * recordSize
                + (blocks * (crcSize + recordSize));

        buffer = ByteBuffer.allocate(writeBufferSize);
    }

    protected int getWriteBufferSize() {
        return buffer.capacity();
    }

    public int flush(final OutputStream out) throws IOException {
        // TODO: 10/07/2020 pad with the last record if there are less than N records (128)
        final int bytes = buffer.position();
        out.write(buffer.array(), 0, bytes);
        out.flush();
        buffer.clear();
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
        // TODO: 10/07/2020 add crc etc, at the end if required
        return address;
    }
}
