package com.clevertap.stormdb;

import static com.clevertap.stormdb.StormDB.CRC_SIZE;
import static com.clevertap.stormdb.StormDB.KEY_SIZE;
import static com.clevertap.stormdb.StormDB.RECORDS_PER_BLOCK;
import static com.clevertap.stormdb.StormDB.RESERVED_KEY_MARKER;

import com.clevertap.stormdb.exceptions.ReadOnlyBufferException;
import com.clevertap.stormdb.exceptions.ValueSizeTooLargeException;
import com.clevertap.stormdb.utils.RecordUtil;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.CRC32;

/**
 * The {@link Buffer} is a logical extension of the WAL file. For a random get, if the index
 * points to an offset greater than that of the actual WAL file, then it's assumed to be in the
 * write buffer.
 */
public class Buffer {

    // TODO: 16/07/20 Rename and make this configurable
    protected static final int FOUR_MB = 4 * 1024 * 1024;

    /**
     * Theoretically, this can go somewhere up to 15 MB, however, the corresponding buffer size will
     * be 1.5 GB. We leave this to 512 KB since it's a fairly high value.
     * <p>
     * Note: The hard limit is due to the fact that {@link ByteBuffer} accepts an int as its size.
     */
    protected static final int MAX_VALUE_SIZE = 512 * 1024;

    private ByteBuffer byteBuffer;
    private final int valueSize;
    private final int recordSize;
    private final boolean readOnly;
    private final int maxRecords;

    /**
     * Initialises a write buffer for the WAL file with the following specification:
     * <ol>
     *     <li>Calculates how many records can fit within a 4 MB buffer</li>
     *     <li>If it turns out to be less than {@link StormDB#RECORDS_PER_BLOCK}, it chooses 128
     *     (this will happen for very large values)</li>
     *     <li>Now, make this a multiple of 128</li>
     *     <li>Then calculate how many CRCs and sync markers need to be accommodated</li>
     *     <li>Finally, initialise a write buffer of the sum of bytes required</li>
     * </ol>
     *
     * @param valueSize The size of each value in this database
     */
    public Buffer(final int valueSize, final boolean readOnly) {
        this.valueSize = valueSize;
        this.recordSize = valueSize + KEY_SIZE;
        this.readOnly = readOnly;
        if (valueSize > MAX_VALUE_SIZE) {
            throw new ValueSizeTooLargeException();
        }

        this.maxRecords = calculateMaxRecords(valueSize);

        final int blocks = this.maxRecords / RECORDS_PER_BLOCK;

        // Each block will have 1 CRC and 1 sync marker (the sync marker is one kv pair)
        final int writeBufferSize = blocks * RECORDS_PER_BLOCK * recordSize
                + (blocks * (CRC_SIZE + recordSize));

        byteBuffer = ByteBuffer.allocate(writeBufferSize);
    }

    public int capacity() {
        return byteBuffer.capacity();
    }

    public static int calculateMaxRecords(final int valueSize) {
        final int recordSize = valueSize + KEY_SIZE;
        int recordsToBuffer = Math.max(FOUR_MB / recordSize, RECORDS_PER_BLOCK);

        // Get to the nearest multiple of 128.
        recordsToBuffer = (recordsToBuffer / RECORDS_PER_BLOCK) * RECORDS_PER_BLOCK;
        return recordsToBuffer;
    }

    public int getMaxRecords() {
        return maxRecords;
    }

    protected int getWriteBufferSize() {
        return byteBuffer.capacity();
    }

    public int flush(final OutputStream out) throws IOException {
        if (readOnly) {
            throw new ReadOnlyBufferException("Initialised in read only mode!");
        }

        if (byteBuffer.position() == 0) {
            return 0;
        }

        // Fill the block with the last record, if required.
        while ((RecordUtil.addressToIndex(recordSize, byteBuffer.position()))
                % RECORDS_PER_BLOCK != 0) {
            final int key = byteBuffer.getInt(byteBuffer.position() - recordSize);
            add(key, byteBuffer.array(), byteBuffer.position() - recordSize + KEY_SIZE);
        }

        final int bytes = byteBuffer.position();
        out.write(byteBuffer.array(), 0, bytes);
        out.flush();
        return bytes;
    }

    public void readFromFiles(List<RandomAccessFile> files,
            final Consumer<ByteBuffer> recordConsumer, final boolean reverse) throws IOException {
        for (RandomAccessFile file : files) {
            readFromFile(file, recordConsumer, reverse);
        }
    }

    public void readFromFile(final RandomAccessFile file, final Consumer<ByteBuffer> recordConsumer,
            final boolean reverse)
            throws IOException {
        final int blockSize = RecordUtil.blockSizeWithTrailer(recordSize);

        if (reverse) {
            while (file.getFilePointer() != 0) {
                byteBuffer.clear();
                final long validBytesRemaining = file.getFilePointer() - byteBuffer.capacity();
                file.seek(Math.max(validBytesRemaining, 0));

                fillBuffer(file, recordConsumer, true);

                // Set the position again, since the read op moved the cursor ahead.
                file.seek(Math.max(validBytesRemaining, 0));
            }
        } else {
            while (true) {
                byteBuffer.clear();
                final int bytesRead = fillBuffer(file, recordConsumer, false);
                if (bytesRead < blockSize) {
                    break;
                }
            }
        }
    }

    private int fillBuffer(RandomAccessFile file, Consumer<ByteBuffer> recordConsumer,
            boolean reverse)
            throws IOException {
        final int bytesRead = file.read(byteBuffer.array());
        if (bytesRead == -1) { // No more data.
            return 0;
        }
        byteBuffer.position(bytesRead);
        byteBuffer.limit(bytesRead);

        // Note: There's the possibility that we'll read the head of the file twice,
        // but that's okay, since we iterate in a backwards fashion.
        final Enumeration<ByteBuffer> iterator = iterator(reverse);
        while (iterator.hasMoreElements()) {
            recordConsumer.accept(iterator.nextElement());
        }

        return bytesRead;
    }

    public byte[] array() {
        return byteBuffer.array();
    }

    public boolean isDirty() {
        return byteBuffer.position() > 0;
    }

    public boolean isFull() {
        return byteBuffer.remaining() == 0; // Perfect alignment, so this works.
    }

    public int add(int key, byte[] value, int valueOffset) {
        if (readOnly) {
            throw new ReadOnlyBufferException("Initialised in read only mode!");
        }

        if (byteBuffer.position() % RecordUtil.blockSizeWithTrailer(recordSize) == 0) {
            insertSyncMarker();
        }

        final int address = byteBuffer.position();

        byteBuffer.putInt(key);
        byteBuffer.put(value, valueOffset, valueSize);

        // Should we close this block?
        // Don't close the block if the we're adding the sync marker kv pair.
        final int nextRecordIndex = RecordUtil.addressToIndex(recordSize, byteBuffer.position());
        if (nextRecordIndex % RECORDS_PER_BLOCK == 0) {
            closeBlock();
        }
        return address;
    }

    /**
     * Always call this from a synchronised context, since it will provide a snapshot of data in the
     * current buffer.
     */
    public Enumeration<ByteBuffer> iterator(final boolean reverse) {
        final ByteBuffer ourBuffer = byteBuffer.duplicate();

        final int recordsToRead;
        if (byteBuffer.position() > 0) {
            recordsToRead = RecordUtil.addressToIndex(recordSize, byteBuffer.position());
        } else {
            recordsToRead = 0;
        }

        return new Enumeration<ByteBuffer>() {
            int currentRecordIndex = reverse ? recordsToRead : 0;

            @Override
            public boolean hasMoreElements() {
                if (reverse) {
                    return currentRecordIndex != 0;
                } else {
                    return currentRecordIndex < recordsToRead;
                }
            }

            @Override
            public ByteBuffer nextElement() {
                final int position;
                if (reverse) {
                    position = (int) RecordUtil.indexToAddress(recordSize, --currentRecordIndex);
                } else {
                    position = (int) RecordUtil.indexToAddress(recordSize, currentRecordIndex++);
                }
                ourBuffer.position(position);
                return ourBuffer;
            }
        };
    }

    private void closeBlock() {
        final CRC32 crc32 = new CRC32();
        final int blockSize = recordSize * RECORDS_PER_BLOCK;
        crc32.update(byteBuffer.array(), byteBuffer.position() - blockSize, blockSize);
        byteBuffer.putInt((int) crc32.getValue());
    }

    public static byte[] getSyncMarker(final int valueSize) {
        final ByteBuffer syncMarker = ByteBuffer.allocate(valueSize + KEY_SIZE);
        Arrays.fill(syncMarker.array(), (byte) 0xFF);
        syncMarker.putInt(RESERVED_KEY_MARKER);  // This will override the first four bytes.
        return syncMarker.array();
    }

    protected void insertSyncMarker() {
        byteBuffer.put(getSyncMarker(valueSize));
    }

    public void clear() {
        byteBuffer = ByteBuffer.allocate(byteBuffer.capacity());
    }
}
