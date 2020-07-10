package com.clevertap.stormdb.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Created by Jude Pereira, at 15:19 on 10/07/2020.
 */
class RecordUtilTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void indexToAddress(final boolean wal) {
        assertEquals(wal ? 0 : 10, RecordUtil.indexToAddress(10, 0, wal));
        assertEquals(wal ? 10 : 20, RecordUtil.indexToAddress(10, 1, wal));
        assertEquals(wal ? 127 * 10 : 127 * 10 + 10, RecordUtil.indexToAddress(10, 127, wal));
        assertEquals(wal ? 128 * 10 + 10 + 4 : 128 * 10 + 20 + 4,
                RecordUtil.indexToAddress(10, 128, wal));

        // Test for record indices which cross the Integer.MAX_VALUE limit.
        assertEquals(wal ? 21709717480L - 10 : 21709717480L,
                RecordUtil.indexToAddress(10, Integer.MAX_VALUE - 1, wal));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void addressToIndex(final boolean wal) {
        assertEquals(0, RecordUtil.addressToIndex(10, wal ? 0 : 10, wal));
        assertEquals(127, RecordUtil.addressToIndex(10, wal ? 1270 : 1270 + 10, wal));
        assertEquals(128,
                RecordUtil.addressToIndex(10, wal ? 1280 + 4 + 10 : 1280 + 2 * 10 + 4, wal));

        // Large addresses.
        assertEquals(Integer.MAX_VALUE - 1, RecordUtil.addressToIndex(10,
                wal ? 21709717480L - 10 : 21709717480L, wal));
    }
}