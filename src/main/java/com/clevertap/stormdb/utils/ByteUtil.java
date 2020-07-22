package com.clevertap.stormdb.utils;

import java.util.Deque;

/**
 * Created by Jude Pereira, at 14:35 on 09/07/2020.
 */
public class ByteUtil {

    private ByteUtil() {
    }

    /**
     * Translates 4 bytes from the given byte array into an integer.
     *
     * @param data   A byte array
     * @param offset The offset for the first byte of the integer
     * @return An integer representation of the 4 byte sequence
     */
    public static int toInt(final byte[] data, final int offset) {
        return data[offset] << 24
                | ((data[offset + 1] & 0xFF) << 16)
                | ((data[offset + 2] & 0xFF) << 8)
                | data[offset + 3] & 0xFF;
    }

    public static boolean arrayEquals(byte[] primitiveBytes, Deque<Byte> bytes) {
        if (bytes == null || primitiveBytes == null) {
            return false;
        }

        if (bytes.size() != primitiveBytes.length) {
            return false;
        }

        final int[] idx = {0};
        boolean[] matched = {true};

        bytes.forEach(aByte -> {
            if (!matched[0]) {
                return;
            }

            if (aByte != primitiveBytes[idx[0]++]) {
                matched[0] = false;
            }
        });

        return matched[0];
    }
}
