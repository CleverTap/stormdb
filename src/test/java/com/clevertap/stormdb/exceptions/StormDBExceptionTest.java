package com.clevertap.stormdb.exceptions;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Created by Jude Pereira, at 14:47 on 22/07/2020.
 */
class StormDBExceptionTest {

    @Test
    void testConstructor() {
        assertThrows(StormDBException.class, () -> {
            throw new StormDBException("foo");
        });
    }
}