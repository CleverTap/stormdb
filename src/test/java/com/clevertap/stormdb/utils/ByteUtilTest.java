package com.clevertap.stormdb.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/**
 * Created by Jude Pereira, at 14:41 on 09/07/2020.
 */
class ByteUtilTest {

    @Test
    void toInt() {
        int[] ints = new int[]{Integer.MIN_VALUE, Integer.MAX_VALUE, 0, -1, 1, 28,
                Integer.MAX_VALUE / 2};
        for (int anInt : ints) {
            final ByteBuffer buf = ByteBuffer.allocate(8);
            buf.position(2);
            buf.putInt(anInt);
            assertEquals(anInt, ByteUtil.toInt(buf.array(), 2));
        }
    }
}