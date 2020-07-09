package com.clevertap.stormdb.exceptions;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Created by Jude Pereira, at 14:54 on 09/07/2020.
 */
class ReservedKeyExceptionTest {

    @Test
    void checkMessage() {
        final ReservedKeyException ex = new ReservedKeyException(Integer.MAX_VALUE);
        assertTrue(ex.getMessage().contains("0x7fffffff"));
    }
}