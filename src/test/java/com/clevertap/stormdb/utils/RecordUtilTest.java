package com.clevertap.stormdb.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Created by Jude Pereira, at 15:19 on 10/07/2020.
 */
class RecordUtilTest {

    @Test
    void indexToAddress() {
        assertEquals(10, RecordUtil.indexToAddress(10, 0));
        assertEquals(20, RecordUtil.indexToAddress(10, 1));
        assertEquals(127 * 10 + 10, RecordUtil.indexToAddress(10, 127));
        assertEquals(128 * 10 + 20 + 4, RecordUtil.indexToAddress(10, 128));

        // Test for record indices which cross the Integer.MAX_VALUE limit.
        assertEquals(21709717480L, RecordUtil.indexToAddress(10, Integer.MAX_VALUE - 1));
    }

    @Test
    void addressToIndex() {
        assertEquals(0, RecordUtil.addressToIndex(10, 10));
        assertEquals(127, RecordUtil.addressToIndex(10, 1270 + 10));
        assertEquals(128, RecordUtil.addressToIndex(10, 1280 + 2 * 10 + 4));

        // Large addresses.
        assertEquals(Integer.MAX_VALUE - 1, RecordUtil.addressToIndex(10, 21709717480L));
    }
}