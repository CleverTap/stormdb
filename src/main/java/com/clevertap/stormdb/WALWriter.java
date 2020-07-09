package com.clevertap.stormdb;

import com.clevertap.stormdb.exceptions.ValueSizeTooLargeException;
import java.nio.ByteBuffer;

/**
 * Created by Jude Pereira, at 16:42 on 09/07/2020.
 */
public class WALWriter {

    protected static final int RECORDS_PER_BLOCK = 128;
    protected static final int FOUR_MB = 4 * 1024 * 1024;

    /**
     * Theoretically, this can go somewhere up to 15 MB, however, the corresponding buffer size will
     * be 1.5 GB. We leave this to 512 KB since it's a fairly high value.
     * <p>
     * Note: The hard limit is due to the fact that {@link ByteBuffer} accepts an int as its size.
     */
    protected static final int MAX_VALUE_SIZE = 512 * 1024;

    private final ByteBuffer writeBuffer;

    /**
     * Initialises a write buffer for the WAL file with the following specification:
     * <ol>
     *     <li>Calculates how many records can fit within a 4 MB buffer</li>
     *     <li>If it turns out to be less than {@link #RECORDS_PER_BLOCK}, it chooses 128
     *     (this will happen for very large values)</li>
     *     <li>Now, make this a multiple of 128</li>
     *     <li>Then calculate how many CRCs and sync markers need to be accommodated</li>
     *     <li>Finally, initialise a write buffer of the sum of bytes required</li>
     * </ol>
     *
     * @param valueSize The size of each value in this database
     */
    public WALWriter(final int valueSize) throws ValueSizeTooLargeException {
        if (valueSize > MAX_VALUE_SIZE) {
            throw new ValueSizeTooLargeException();
        }

        final int recordSize = valueSize + 4; // Keys are always 4 bytes.
        int recordsToBuffer = Math.max(FOUR_MB / recordSize, RECORDS_PER_BLOCK);

        // Get to the nearest multiple of 128.
        recordsToBuffer = (recordsToBuffer / RECORDS_PER_BLOCK) * RECORDS_PER_BLOCK;

        final int blocks = recordsToBuffer / RECORDS_PER_BLOCK;
        final int crcSize = 4;
        // Each block will have 1 CRC and 1 sync marker (the sync marker is one kv pair)
        final int writeBufferSize = blocks * RECORDS_PER_BLOCK * recordSize
                + (blocks * (crcSize + recordSize));

        writeBuffer = ByteBuffer.allocate(writeBufferSize);
    }

    protected int getWriteBufferSize() {
        return writeBuffer.capacity();
    }
}
