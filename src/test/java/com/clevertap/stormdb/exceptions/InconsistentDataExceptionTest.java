package com.clevertap.stormdb.exceptions;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Created by Jude Pereira, at 14:45 on 22/07/2020.
 */
class InconsistentDataExceptionTest {

    @Test
    void testConstructor() {
        assertThrows(InconsistentDataException.class, () -> {
            throw new InconsistentDataException();
        });
    }
}