package com.clevertap.stormdb;

import com.clevertap.stormdb.utils.BitSetLong;

import java.io.File;
import java.util.BitSet;

class CompactionState {

    private final long start = System.currentTimeMillis();

    long nextFileRecordIndex;

    BitSetLong dataInNextFile = new BitSetLong();  // REVIEW : we can change these to some Set<Long> or any other set that accpets long ?
    BitSetLong dataInNextWalFile = new BitSetLong();

    File nextWalFile;
    File nextDataFile;

    boolean runningForTooLong() {
        return System.currentTimeMillis() - start > 30 * 60 * 1000;
    }

    public long getStart() {
        return start;
    }
}
