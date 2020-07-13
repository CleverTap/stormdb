package com.clevertap.stormdb;

import static com.clevertap.stormdb.StormDB.RECORDS_PER_BLOCK;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.clevertap.stormdb.exceptions.ValueSizeTooLargeException;
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Created by Jude Pereira, at 17:52 on 09/07/2020.
 */
class BufferTest {

    @Test
    void checkWriteBufferSize() throws ValueSizeTooLargeException {
        // Small values.
        assertEquals(4235400, new Buffer(10).getWriteBufferSize());
        assertEquals(4252897, new Buffer(1).getWriteBufferSize());
        assertEquals(4229316, new Buffer(36).getWriteBufferSize());
        assertEquals(4111096, new Buffer(1024).getWriteBufferSize());

        // Large values have a consequence in memory management.
        assertEquals(2114056, new Buffer(16 * 1024).getWriteBufferSize());
        assertEquals(16908808, new Buffer(128 * 1024).getWriteBufferSize());
        assertEquals(33817096, new Buffer(256 * 1024).getWriteBufferSize());
        assertEquals(67633672, new Buffer(512 * 1024).getWriteBufferSize());
    }

    @Test
    void checkWriteBufferSizeTooLarge() {
        assertThrows(ValueSizeTooLargeException.class, () -> new Buffer(512 * 1024 + 1));
    }

    @Test
    void verifyIncompleteBlockPadding() throws ValueSizeTooLargeException, IOException {
        final AtomicInteger recordsAdded = new AtomicInteger();
        final AtomicInteger syncMarkersAdded = new AtomicInteger();
        final Buffer buffer = new Buffer(100) {
            @Override
            public int add(int key, byte[] value, int valueOffset) {
                if (key == StormDB.RESERVED_KEY_MARKER) {
                    syncMarkersAdded.incrementAndGet();
                } else {
                    recordsAdded.incrementAndGet();
                }
                return super.add(key, value, valueOffset);
            }
        };

        buffer.add(28, new byte[100], 0);

        assertEquals(1, recordsAdded.get());
        assertEquals(0, syncMarkersAdded.get());

        buffer.flush(new ByteArrayOutputStream());

        // Although we've added just one record, #add should be called
        // 128 more times, bringing the total records to 129 (+1 for the sync marker).
        assertEquals(128, recordsAdded.get());
        assertEquals(1, syncMarkersAdded.get());
    }

    @Test
    void verifyBlockTrailer() throws ValueSizeTooLargeException, IOException {
        final Buffer buffer = new Buffer(100);
        final CRC32 crc32 = new CRC32();

        for (int i = 0; i < RECORDS_PER_BLOCK; i++) {
            final byte[] value = new byte[100];
            ThreadLocalRandom.current().nextBytes(value);
            crc32.update(i >> 24);
            crc32.update(i >> 16);
            crc32.update(i >> 8);
            crc32.update(i);
            crc32.update(value);
            buffer.add(i, value, 0);
        }

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        buffer.flush(out);

        final ByteBuffer bytesWritten = ByteBuffer.wrap(out.toByteArray());


        // Verify CRC32 checksum.
        bytesWritten.position(RECORDS_PER_BLOCK * 104);
        assertNotEquals(0, crc32.getValue());
        assertEquals((int) crc32.getValue(), bytesWritten.getInt());

        // Verify the sync marker.
        assertEquals(StormDB.RESERVED_KEY_MARKER, bytesWritten.getInt());
        final byte[] syncMarkerExpectedValue = new byte[100];
        Arrays.fill(syncMarkerExpectedValue, (byte) 0xFF);
        final byte[] syncMarkerActualValue = new byte[100];
        bytesWritten.get(syncMarkerActualValue);
        assertArrayEquals(syncMarkerExpectedValue, syncMarkerActualValue);

        // Ensure that nothing else was written.
        assertFalse(bytesWritten.hasRemaining());
    }

    @Test
    void verifyDirty() throws ValueSizeTooLargeException {
        final Buffer buf = new Buffer(100);
        assertFalse(buf.isDirty());
        buf.add(10, new byte[100], 0);
        assertTrue(buf.isDirty());
    }

    @Test
    void verifyArrayNotNull() throws ValueSizeTooLargeException {
        final Buffer buf = new Buffer(100);
        assertNotNull(buf.array());
    }

    @Test
    void verifyEmptyFlush() throws ValueSizeTooLargeException, IOException {
        final Buffer buf = new Buffer(100);
        final ByteOutputStream out = new ByteOutputStream();
        assertEquals(0, buf.flush(out));
        assertEquals(0, out.getCount());
    }

    @Test
    void verifyFull() throws ValueSizeTooLargeException {
        final Buffer buf = new Buffer(100);
        assertFalse(buf.isFull());

        for (int i = 0; i < buf.getMaxRecords(); i++) {
            buf.add(i, new byte[100], 0);
        }

        assertTrue(buf.isFull());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1,
            RECORDS_PER_BLOCK - 1,
            RECORDS_PER_BLOCK,
            RECORDS_PER_BLOCK + 1,
            100, 1000, 10_000, 100_000, 200_000, 349_440})
    void iterator(final int records) throws ValueSizeTooLargeException {
        final Buffer buffer = new Buffer(8);

        // Add N records.
        for (int i = 0; i < records; i++) {
            if (buffer.isFull()) {
                throw new AssertionError(
                        "Too many values for test case! Requested: " + records + ", but only "
                                + buffer.getMaxRecords() + " are possible!");
            }
            final ByteBuffer value = ByteBuffer.allocate(8);
            value.putLong((long) i * Integer.MAX_VALUE + 1);
            buffer.add(i, value.array(), 0);
        }

        final Enumeration<ByteBuffer> iterator = buffer.iterator();

        assertEquals(records > 0, iterator.hasMoreElements());
        final ArrayList<Integer> keysReceivedOrder = new ArrayList<>();
        while (iterator.hasMoreElements()) {
            final ByteBuffer byteBuffer = iterator.nextElement();
            final int key = byteBuffer.getInt();
            final long value = byteBuffer.getLong();
            assertEquals((long) key * Integer.MAX_VALUE + 1, value);
            keysReceivedOrder.add(key);
        }

        assertEquals(records, keysReceivedOrder.size());

        final ArrayList<Integer> expectedKeysReceivedOrder = new ArrayList<>();

        for (int i = records - 1; i >= 0; i--) {
            expectedKeysReceivedOrder.add(i);
        }

        assertArrayEquals(expectedKeysReceivedOrder.toArray(), keysReceivedOrder.toArray());
    }
}