package com.clevertap.stormdb;

import static com.clevertap.stormdb.StormDBConfig.KEY_SIZE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.clevertap.stormdb.exceptions.ReadOnlyBufferException;
import com.clevertap.stormdb.exceptions.ValueSizeTooLargeException;
import com.clevertap.stormdb.utils.RecordUtil;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Created by Jude Pereira, at 17:52 on 09/07/2020.
 */
class BufferTest {

    // If we want to parallelize tests, create new dbConfig everytime new buffer is called.
    private static final StormDBConfig dbConfig = new StormDBConfig();

    private static Buffer newBuffer(final int valueSize, final boolean readOnly) {
        dbConfig.valueSize = valueSize;
        return new Buffer(dbConfig, readOnly);
    }

    private static Buffer newWriteBuffer(final int valueSize) {
        return newBuffer(valueSize, false);
    }

    private static Buffer newReadBuffer(final int valueSize) {
        return newBuffer(valueSize, true);
    }

    @Test
    void checkWriteBufferSize() throws ValueSizeTooLargeException {
        // Small values.
        assertEquals(4235400, newWriteBuffer(10).getWriteBufferSize());
        assertEquals(4252897, newWriteBuffer(1).getWriteBufferSize());
        assertEquals(4229316, newWriteBuffer(36).getWriteBufferSize());
        assertEquals(4111096, newWriteBuffer(1024).getWriteBufferSize());

        // Large values have a consequence in memory management.
        assertEquals(2114056, newWriteBuffer(16 * 1024).getWriteBufferSize());
        assertEquals(16908808, newWriteBuffer(128 * 1024).getWriteBufferSize());
        assertEquals(33817096, newWriteBuffer(256 * 1024).getWriteBufferSize());
        assertEquals(67633672, newWriteBuffer(512 * 1024).getWriteBufferSize());
    }

    @Test
    void checkWriteBufferSizeTooLarge() {
        assertThrows(ValueSizeTooLargeException.class, () -> newWriteBuffer(512 * 1024 + 1));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 8, 100})
    void verifyIncompleteBlockPadding(final int valueSize)
            throws ValueSizeTooLargeException, IOException {

        final byte[] expectedValue = new byte[valueSize];
        ThreadLocalRandom.current().nextBytes(expectedValue);

        final AtomicInteger recordsAdded = new AtomicInteger();
        final AtomicInteger syncMarkersAdded = new AtomicInteger();
        dbConfig.valueSize = valueSize; // Create new dbConfig for parallel tests.
        final Buffer buffer = new Buffer(dbConfig, false) {
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
        assertEquals(1, syncMarkersAdded.get());

        buffer.flush(new ByteArrayOutputStream());

        // Although we've added just one record, #add should be called
        // 127 more times, bringing the total records to 128.
        assertEquals(128, recordsAdded.get());
        assertEquals(1, syncMarkersAdded.get());
    }

    @Test
    void verifyBlockTrailer() throws ValueSizeTooLargeException, IOException {
        final int valueSize = 100;
        final int recordSize = valueSize + KEY_SIZE;
        final Buffer buffer = newWriteBuffer(valueSize);
        final CRC32 crc32 = new CRC32();

        for (int i = 0; i < StormDBConfig.RECORDS_PER_BLOCK; i++) {
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
        bytesWritten.position(StormDBConfig.RECORDS_PER_BLOCK * recordSize + recordSize);
        assertNotEquals(0, crc32.getValue());
        assertEquals((int) crc32.getValue(), bytesWritten.getInt());

        // Verify the sync marker.
        bytesWritten.position(0);

        assertEquals(StormDB.RESERVED_KEY_MARKER, bytesWritten.getInt());
        final byte[] syncMarkerExpectedValue = new byte[valueSize];
        Arrays.fill(syncMarkerExpectedValue, (byte) 0xFF);
        final byte[] syncMarkerActualValue = new byte[valueSize];
        bytesWritten.get(syncMarkerActualValue);
        assertArrayEquals(syncMarkerExpectedValue, syncMarkerActualValue);

        // Ensure that nothing else was written.
        assertEquals(
                StormDBConfig.RECORDS_PER_BLOCK * recordSize + StormDBConfig.CRC_SIZE + recordSize,
                bytesWritten.capacity());
    }

    @Test
    void verifyDirty() throws ValueSizeTooLargeException {
        final Buffer buf = newWriteBuffer(100);
        assertFalse(buf.isDirty());
        buf.add(10, new byte[100], 0);
        assertTrue(buf.isDirty());
    }

    @Test
    void verifyArrayNotNull() throws ValueSizeTooLargeException {
        final Buffer buf = newWriteBuffer(100);
        assertNotNull(buf.array());
    }

    @Test
    void verifyEmptyFlush() throws ValueSizeTooLargeException, IOException {
        final Buffer buf = newWriteBuffer(100);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertEquals(0, buf.flush(out));
        assertEquals(0, out.size());
    }

    @Test
    void verifyFull() throws ValueSizeTooLargeException {
        final Buffer buf = newWriteBuffer(100);
        assertFalse(buf.isFull());

        for (int i = 0; i < buf.getMaxRecords(); i++) {
            buf.add(i, new byte[100], 0);
        }

        assertTrue(buf.isFull());
    }

    @Test
    void clear() {
        final Buffer buffer = newWriteBuffer(10);
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
            final Buffer buffer = newWriteBuffer(valueSize);
            final int[] recordsArr = {0, 1,
                    StormDBConfig.RECORDS_PER_BLOCK - 1,
                    StormDBConfig.RECORDS_PER_BLOCK,
                    StormDBConfig.RECORDS_PER_BLOCK + 1,
                    100, 1000, 10_000, 100_000, 200_000,
                    buffer.calculateMaxRecords(valueSize)};

            for (int records : recordsArr) {
                for (boolean wal : wals) {
                    for (boolean far : flushAndRead) {
                        if (records <= buffer.calculateMaxRecords(valueSize)) {
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
    void iterator(final int valueSize, final int records, final boolean reverse,
            final boolean flushAndReadFromFile)
            throws ValueSizeTooLargeException, IOException {
        final Buffer buffer = newWriteBuffer(valueSize);

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
            final int address = buffer.add(i, value, 0);
            final int recordSize = valueSize + KEY_SIZE;
            assertEquals(RecordUtil.indexToAddress(recordSize, i), address);
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
            final Buffer tmpBuffer = newReadBuffer(valueSize);
            final RandomAccessFile raf = new RandomAccessFile(tmpFile, "r");
            if (reverse) {
                raf.seek(raf.length());
            }
            tmpBuffer.readFromFile(raf, reverse, byteBuffer -> {
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
            final Enumeration<ByteBuffer> iterator = buffer.iterator(reverse);
            while (iterator.hasMoreElements()) {
                final ByteBuffer byteBuffer = iterator.nextElement();
                recordConsumer.accept(byteBuffer);
            }
        }

        assertEquals(records, keysReceivedOrder.size());

        final ArrayList<Integer> expectedKeysReceivedOrder = new ArrayList<>();

        for (int i = reverse ? records - 1 : 0; reverse ? i >= 0 : i < records;
                i += reverse ? -1 : 1) {
            expectedKeysReceivedOrder.add(i);
        }

        assertArrayEquals(expectedKeysReceivedOrder.toArray(), keysReceivedOrder.toArray());
    }

    @Test
    void addReadOnly() {
        final Buffer buffer = newReadBuffer(10);
        assertThrows(ReadOnlyBufferException.class, () -> buffer.add(0, new byte[10], 0));
    }

    @Test
    void flushReadOnly() {
        final Buffer buffer = newReadBuffer(10);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertThrows(ReadOnlyBufferException.class,
                () -> buffer.flush(out));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void readFromFiles(final boolean reverse) throws IOException {
        final Buffer buffer = Mockito.mock(Buffer.class);
        doCallRealMethod().when(buffer).readFromFiles(any(), anyBoolean(), any());

        final ArrayList<RandomAccessFile> files = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            files.add(Mockito.mock(RandomAccessFile.class));
        }

        final Consumer<ByteBuffer> recordConsumer = byteBuffer -> {
        };

        buffer.readFromFiles(files, reverse, recordConsumer);

        final ArgumentCaptor<RandomAccessFile> rafCaptor = ArgumentCaptor
                .forClass(RandomAccessFile.class);
        //noinspection unchecked
        final ArgumentCaptor<Consumer<ByteBuffer>> rcCaptor = ArgumentCaptor
                .forClass(Consumer.class);
        final ArgumentCaptor<Boolean> reverseCaptor = ArgumentCaptor.forClass(Boolean.class);

        verify(buffer, times(files.size()))
                .readFromFile(rafCaptor.capture(), reverseCaptor.capture(),
                        rcCaptor.capture());

        for (int i = 0; i < files.size(); i++) {
            assertSame(files.get(i), rafCaptor.getAllValues().get(i));
            assertSame(recordConsumer, rcCaptor.getAllValues().get(i));
            assertSame(reverse, reverseCaptor.getAllValues().get(i));
        }
    }
}