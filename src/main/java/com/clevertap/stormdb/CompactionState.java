package com.clevertap.stormdb;

import java.io.File;
import java.util.BitSet;

class CompactionState {

    long nextFileRecordIndex;

    // TODO: 09/07/20 We can get rid of this bitset. revisit.
    BitSet dataInNextFile = new BitSet();
    BitSet dataInNextWalFile = new BitSet();

    File nextWalFile;
    File nextDataFile;

}
