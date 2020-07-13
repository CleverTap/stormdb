package com.clevertap.stormdb.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Created by Jude Pereira, at 14:41 on 09/07/2020.
 */
class ByteUtilTest {

    @ParameterizedTest
    @ValueSource(ints = {Integer.MIN_VALUE,
            Integer.MAX_VALUE, 0, -1, 1, 28,
            Integer.MAX_VALUE / 2})
    void toInt(final int val) {
        final ByteBuffer buf = ByteBuffer.allocate(8);
        buf.position(2);
        buf.putInt(val);
        assertEquals(val, ByteUtil.toInt(buf.array(), 2));
    }
}