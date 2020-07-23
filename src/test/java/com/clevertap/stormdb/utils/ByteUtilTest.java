package com.clevertap.stormdb.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.concurrent.ThreadLocalRandom;
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

    @Test
    void arrayEquals() {
        final ArrayDeque<Byte> arrayDeque = new ArrayDeque<>();
        final byte[] bytes = new byte[100];

        assertFalse(ByteUtil.arrayEquals(bytes, null));
        assertFalse(ByteUtil.arrayEquals(null, arrayDeque));
        assertFalse(ByteUtil.arrayEquals(null, null));
        assertFalse(ByteUtil.arrayEquals(bytes, arrayDeque));

        for (int i = 0; i < bytes.length; i++) {
            arrayDeque.add((byte) 0);
        }

        assertTrue(ByteUtil.arrayEquals(bytes, arrayDeque));

        ThreadLocalRandom.current().nextBytes(bytes);

        assertFalse(ByteUtil.arrayEquals(bytes, arrayDeque));

        arrayDeque.clear();

        for (byte aByte : bytes) {
            arrayDeque.add(aByte);
        }

        assertTrue(ByteUtil.arrayEquals(bytes, arrayDeque));

        arrayDeque.removeFirst();

        assertFalse(ByteUtil.arrayEquals(bytes, arrayDeque));
    }
}