package com.clevertap.stormdb;

import java.io.File;
import java.util.BitSet;

class CompactionState {

    private final long start = System.currentTimeMillis();

    long nextFileRecordIndex;

    BitSet dataInNextFile = new BitSet();
    BitSet dataInNextWalFile = new BitSet();

    File nextWalFile;
    File nextDataFile;
    File nextDeletedKeysFile;

    boolean runningForTooLong() {
        return System.currentTimeMillis() - start > 30 * 60 * 1000;
    }

    public long getStart() {
        return start;
    }
}
