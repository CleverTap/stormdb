package com.clevertap.stormdb;

import static com.clevertap.stormdb.StormDB.CRC_SIZE;
import static com.clevertap.stormdb.StormDB.KEY_SIZE;
import static com.clevertap.stormdb.StormDB.RECORDS_PER_BLOCK;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.clevertap.stormdb.exceptions.ReadOnlyBufferException;
import com.clevertap.stormdb.exceptions.ValueSizeTooLargeException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;
import java.util.zip.CRC32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Created by Jude Pereira, at 17:52 on 09/07/2020.
 */
class BufferTest {
    private static Buffer newBuffer(final int valueSize) {
        return new Buffer(valueSize, false, true);
    }

    @Test
    void checkWriteBufferSize() throws ValueSizeTooLargeException {
        // Small values.
        assertEquals(4235400, newBuffer(10).getWriteBufferSize());
        assertEquals(4252897, newBuffer(1).getWriteBufferSize());
        assertEquals(4229316, newBuffer(36).getWriteBufferSize());
        assertEquals(4111096, newBuffer(1024).getWriteBufferSize());

        // Large values have a consequence in memory management.
        assertEquals(2114056, newBuffer(16 * 1024).getWriteBufferSize());
        assertEquals(16908808, newBuffer(128 * 1024).getWriteBufferSize());
        assertEquals(33817096, newBuffer(256 * 1024).getWriteBufferSize());
        assertEquals(67633672, newBuffer(512 * 1024).getWriteBufferSize());
    }

    @Test
    void checkWriteBufferSizeTooLarge() {
        assertThrows(ValueSizeTooLargeException.class, () -> newBuffer(512 * 1024 + 1));
        // TODO: 13/07/2020 add check for verifying that a buffer is set to new after clear
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 8, 100})
    void verifyIncompleteBlockPadding(final int valueSize)
            throws ValueSizeTooLargeException, IOException {

        final byte[] expectedValue = new byte[valueSize];
        ThreadLocalRandom.current().nextBytes(expectedValue);

        final AtomicInteger recordsAdded = new AtomicInteger();
        final AtomicInteger syncMarkersAdded = new AtomicInteger();
        final Buffer buffer = new Buffer(valueSize, false, true) {
            @Override
            public int add(int key, byte[] value, int valueOffset) {
                recordsAdded.incrementAndGet();
                final byte[] actualValue = new byte[valueSize];
                System.arraycopy(value, valueOffset, actualValue, 0, valueSize);
                assertArrayEquals(expectedValue, actualValue);
                assertEquals(28, key);
                return super.add(key, value, valueOffset);
            }

            @Override
            protected void insertSyncMarker() {
                syncMarkersAdded.incrementAndGet();
                super.insertSyncMarker();
            }
        };

        buffer.add(28, expectedValue, 0);

        assertEquals(1, recordsAdded.get());
        assertEquals(0, syncMarkersAdded.get());

        buffer.flush(new ByteArrayOutputStream());

        // Although we've added just one record, #add should be called
        // 127 more times, bringing the total records to 128.
        assertEquals(128, recordsAdded.get());
        assertEquals(1, syncMarkersAdded.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void verifyBlockTrailer(final boolean wal) throws ValueSizeTooLargeException, IOException {
        final int valueSize = 100;
        final int recordSize = valueSize + KEY_SIZE;
        final Buffer buffer = new Buffer(valueSize, false, wal);
        final CRC32 crc32 = new CRC32();

        for (int i = 0; i < RECORDS_PER_BLOCK; i++) {
            final byte[] value = new byte[valueSize];
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
        if (wal) {
            bytesWritten.position(RECORDS_PER_BLOCK * recordSize);
        } else {
            bytesWritten.position(RECORDS_PER_BLOCK * recordSize + recordSize);
        }
        assertNotEquals(0, crc32.getValue());
        assertEquals((int) crc32.getValue(), bytesWritten.getInt());

        // Verify the sync marker.
        if (wal) {
            bytesWritten.position(RECORDS_PER_BLOCK * recordSize + CRC_SIZE);
        } else {
            bytesWritten.position(0);
        }

        assertEquals(StormDB.RESERVED_KEY_MARKER, bytesWritten.getInt());
        final byte[] syncMarkerExpectedValue = new byte[valueSize];
        Arrays.fill(syncMarkerExpectedValue, (byte) 0xFF);
        final byte[] syncMarkerActualValue = new byte[valueSize];
        bytesWritten.get(syncMarkerActualValue);
        assertArrayEquals(syncMarkerExpectedValue, syncMarkerActualValue);

        // Ensure that nothing else was written.
        assertEquals(RECORDS_PER_BLOCK * recordSize + CRC_SIZE + recordSize,
                bytesWritten.capacity());
    }

    @Test
    void verifyDirty() throws ValueSizeTooLargeException {
        final Buffer buf = newBuffer(100);
        assertFalse(buf.isDirty());
        buf.add(10, new byte[100], 0);
        assertTrue(buf.isDirty());
    }

    @Test
    void verifyArrayNotNull() throws ValueSizeTooLargeException {
        final Buffer buf = newBuffer(100);
        assertNotNull(buf.array());
    }

    @Test
    void verifyEmptyFlush() throws ValueSizeTooLargeException, IOException {
        final Buffer buf = newBuffer(100);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertEquals(0, buf.flush(out));
        assertEquals(0, out.size());
    }

    @Test
    void verifyFull() throws ValueSizeTooLargeException {
        final Buffer buf = newBuffer(100);
        assertFalse(buf.isFull());

        for (int i = 0; i < buf.getMaxRecords(); i++) {
            buf.add(i, new byte[100], 0);
        }

        assertTrue(buf.isFull());
    }

    @Test
    void clear() {
        final Buffer buffer = new Buffer(10, false, true);
        final byte[] oldArray = buffer.array();
        buffer.clear();
        assertNotSame(oldArray, buffer.array());
    }

    private static Stream<Arguments> provideIteratorTestCases() {
        final int[] valueSizes = {1, 2, 4, 8, 16, 32, 64, 128, 512, 1024, 2048, 4096};
        final boolean[] wals = {true, false}; // true for WAL, false for data.
        final boolean[] flushAndRead = {true, false};

        final Builder<Arguments> builder = Stream.builder();

        for (int valueSize : valueSizes) {
            final int[] recordsArr = {0, 1,
                    RECORDS_PER_BLOCK - 1,
                    RECORDS_PER_BLOCK,
                    RECORDS_PER_BLOCK + 1,
                    100, 1000, 10_000, 100_000, 200_000,
                    Buffer.calculateMaxRecords(valueSize)};

            for (int records : recordsArr) {
                for (boolean wal : wals) {
                    for (boolean far : flushAndRead) {
                        if (records <= Buffer.calculateMaxRecords(valueSize)) {
                            builder.accept(Arguments.of(valueSize, records, wal, far));
                            builder.accept(Arguments.of(valueSize, records, wal, far));
                        }
                    }

                }
            }
        }

        return builder.build();
    }
    

    @ParameterizedTest
    @MethodSource("provideIteratorTestCases")
    void iterator(final int valueSize, final int records, final boolean wal,
            final boolean flushAndReadFromFile)
            throws ValueSizeTooLargeException, IOException {
        final Buffer buffer = new Buffer(valueSize, false, wal);

        final HashMap<Integer, byte[]> expectedMap = new HashMap<>();


        // Add N records.
        for (int i = 0; i < records; i++) {
            if (buffer.isFull()) {
                throw new AssertionError(
                        "Too many values for test case! Requested: " + records + ", but only "
                                + buffer.getMaxRecords() + " are possible!");
            }
            final byte[] value = new byte[valueSize];
            ThreadLocalRandom.current().nextBytes(value);
            buffer.add(i, value, 0);
            expectedMap.put(i, value);
        }

        final ArrayList<Integer> keysReceivedOrder = new ArrayList<>();

        final Consumer<ByteBuffer> recordConsumer = byteBuffer -> {
            final int key = byteBuffer.getInt();
            final byte[] actualValue = new byte[valueSize];
            byteBuffer.get(actualValue);
            assertArrayEquals(expectedMap.get(key), actualValue);
            keysReceivedOrder.add(key);
        };

        if (flushAndReadFromFile) {
            final BitSet dupCheck = new BitSet();
            final Path tmpPath = Files.createTempFile("stormdb_", "_buffer");
            final File tmpFile = tmpPath.toFile();
            tmpFile.deleteOnExit();
            final FileOutputStream out = new FileOutputStream(tmpFile);
            buffer.flush(out);
            out.flush();
            out.close();
            final Buffer tmpBuffer = new Buffer(valueSize, true, wal);
            final RandomAccessFile raf = new RandomAccessFile(tmpFile, "r");
            if (wal) {
                raf.seek(raf.length());
            }
            tmpBuffer.readFromFile(raf, byteBuffer -> {
                // Since we're reading from disk, we might hit the same key more than once.
                // This is due to the fact that the last record in a buffer is duplicated
                // up to a total of 127 times, to make all blocks in the buffer a multiple
                // of 128.

                final int key = byteBuffer.getInt();
                byteBuffer.position(byteBuffer.position() - 4);
                if (!dupCheck.get(key)) {
                    dupCheck.set(key);
                    recordConsumer.accept(byteBuffer);
                }
            });
        } else {
            final Enumeration<ByteBuffer> iterator = buffer.iterator();
            while (iterator.hasMoreElements()) {
                final ByteBuffer byteBuffer = iterator.nextElement();
                recordConsumer.accept(byteBuffer);
            }
        }

        assertEquals(records, keysReceivedOrder.size());

        final ArrayList<Integer> expectedKeysReceivedOrder = new ArrayList<>();

        for (int i = wal ? records - 1 : 0; wal ? i >= 0 : i < records; i += wal ? -1 : 1) {
            expectedKeysReceivedOrder.add(i);
        }

        assertArrayEquals(expectedKeysReceivedOrder.toArray(), keysReceivedOrder.toArray());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void addReadOnly(final boolean wal) {
        final Buffer buffer = new Buffer(10, true, wal);
        assertThrows(ReadOnlyBufferException.class, () -> buffer.add(0, new byte[10], 0));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void flushReadOnly(final boolean wal) {
        final Buffer buffer = new Buffer(10, true, wal);
        assertThrows(ReadOnlyBufferException.class, () -> buffer.flush(new ByteArrayOutputStream()));
    }
}