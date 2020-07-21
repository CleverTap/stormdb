package com.clevertap.stormdb;

import java.io.File;
import java.util.BitSet;

class CompactionState {

    long nextFileRecordIndex;

    BitSet dataInNextFile = new BitSet();
    BitSet dataInNextWalFile = new BitSet();

    File nextWalFile;
    File nextDataFile;

}
