package com.clevertap.stormdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.clevertap.stormdb.exceptions.ValueSizeTooLargeException;
import org.junit.jupiter.api.Test;

/**
 * Created by Jude Pereira, at 17:52 on 09/07/2020.
 */
class WALWriterTest {

    @Test
    void checkWriteBufferSize() throws ValueSizeTooLargeException {
        // Small values.
        assertEquals(4235400, new WALWriter(10).getWriteBufferSize());
        assertEquals(4252897, new WALWriter(1).getWriteBufferSize());
        assertEquals(4229316, new WALWriter(36).getWriteBufferSize());
        assertEquals(4111096, new WALWriter(1024).getWriteBufferSize());

        // Large values have a consequence in memory management.
        assertEquals(2114056, new WALWriter(16 * 1024).getWriteBufferSize());
        assertEquals(16908808, new WALWriter(128 * 1024).getWriteBufferSize());
        assertEquals(33817096, new WALWriter(256 * 1024).getWriteBufferSize());
        assertEquals(67633672, new WALWriter(512 * 1024).getWriteBufferSize());
    }

    @Test
    void checkWriteBufferSizeTooLarge() {
        assertThrows(ValueSizeTooLargeException.class, () -> new WALWriter(512 * 1024 + 1));
    }
}