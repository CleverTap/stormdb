package com.clevertap.stormdb.utils;

public interface BitSetList {
    int BYTES_IN_ONE_BIT_SET = (Integer.MAX_VALUE / 8);
    int MAX_BITS_IN_BIT_SET = BYTES_IN_ONE_BIT_SET * 8;

    void ensureIdx(int idx);

    void set(long uid);

    boolean get(long uid);

    void unset(long uid);
}

