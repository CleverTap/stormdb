package com.clevertap.stormdb.utils;

import static com.clevertap.stormdb.Config.CRC_SIZE;
import static com.clevertap.stormdb.Config.RECORDS_PER_BLOCK;

/**
 * Helper methods to deal with addressing computations.
 */
public class RecordUtil {

    private RecordUtil() {
    }

    public static int blockSizeWithTrailer(final int recordSize) {
        return recordSize * RECORDS_PER_BLOCK + CRC_SIZE + recordSize;
    }

    public static long indexToAddress(final int recordSize, final long recordIndex) {
        final int blockSize = blockSizeWithTrailer(recordSize);
        final long blocksBefore = recordIndex / RECORDS_PER_BLOCK;
        long address = blocksBefore * blockSize
                + (recordIndex % RECORDS_PER_BLOCK) * recordSize;

        // Account for the sync marker kv pair before the start of the current block.
        address += recordSize;
        return address;
    }

    /**
     * Given an address, it translates it to a record index.
     * <p>
     * See {@link #indexToAddress(int, long)}.
     *
     * @param address The absolute record address
     * @return An index for addressing this record
     */
    public static int addressToIndex(final int recordSize, long address) {
        final int blockSize = blockSizeWithTrailer(recordSize);
        // Account for the sync marker kv pair before the start of the current block.
        address -= recordSize;
        final int blocksBefore = (int) (address / blockSize);
        final int recordInCurrentBlock = (int) ((address % blockSize) / recordSize);
        return blocksBefore * RECORDS_PER_BLOCK + recordInCurrentBlock;
    }
}
