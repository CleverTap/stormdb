package com.clevertap.stormdb.utils;

import static com.clevertap.stormdb.StormDB.CRC_SIZE;
import static com.clevertap.stormdb.StormDB.RECORDS_PER_BLOCK;

/**
 * Helper methods to deal with addressing computations.
 */
public class RecordUtil {

    private RecordUtil() {
    }

    public static int blockSizeWithTrailer(final int recordSize) {
        return recordSize * RECORDS_PER_BLOCK + CRC_SIZE + recordSize;
    }

    public static long indexToAddress(final int recordSize, final int recordIndex,
            final boolean wal) {
        final int blockSize = blockSizeWithTrailer(recordSize);
        final int blocksBefore = recordIndex / RECORDS_PER_BLOCK;
        long address = (long) blocksBefore * blockSize
                + (recordIndex % RECORDS_PER_BLOCK) * recordSize;

        // Account for the sync marker kv pair before the start of the current block.
        if (!wal) {
            address += recordSize;
        }
        return address;
    }

    /**
     * Given an address, it translates it to a record index.
     * <p>
     * See {@link #indexToAddress(int, int, boolean)}.
     *
     * @param address The absolute record address
     * @return An index for addressing this record
     */
    public static int addressToIndex(final int recordSize, final long address, boolean wal) {
        final int blockSize = blockSizeWithTrailer(recordSize);
        final int blocksBefore = (int) (address / blockSize);
        int recordInCurrentBlock = (int) ((address % blockSize) / recordSize);
        if (!wal) {
            recordInCurrentBlock -= 1;
        }
        return blocksBefore * RECORDS_PER_BLOCK + recordInCurrentBlock;
    }
}
