package com.clevertap.stormdb;

import static com.clevertap.stormdb.StormDB.RESERVED_KEY_MARKER;
import static com.clevertap.stormdb.Config.CRC_SIZE;
import static com.clevertap.stormdb.Config.KEY_SIZE;
import static com.clevertap.stormdb.Config.RECORDS_PER_BLOCK;

import com.clevertap.stormdb.exceptions.BufferFullException;
import com.clevertap.stormdb.exceptions.ReadOnlyBufferException;
import com.clevertap.stormdb.exceptions.StormDBRuntimeException;
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
import java.util.ArrayList;

/**
 * The {@link Buffer} is a logical extension of the WAL file. For a random get, if the index points
 * to an offset greater than that of the actual WAL file, then it's assumed to be in the write
 * buffer.
 */
public class Buffer {

    private ByteBuffer byteBuffer;
    private final boolean readOnly;
    private final Config dbConfig;


    /**
     * Initialises a write buffer for the WAL file with the following specification:
     * <ol>
     *     <li>Calculates how many records can fit within a 4 MB buffer</li>
     *     <li>If it turns out to be less than {@link Config#RECORDS_PER_BLOCK}, it chooses 128
     *     (this will happen for very large values)</li>
     *     <li>Now, make this a multiple of 128</li>
     *     <li>Then calculate how many CRCs and sync markers need to be accommodated</li>
     *     <li>Finally, initialise a write buffer of the sum of bytes required</li>
     * </ol>
     *
     * @param dbConfig Configuration using which db instance is produced
     * @param readOnly Whether buffer is read only.
     */
    Buffer(final Config dbConfig, final boolean readOnly) {
        this.readOnly = readOnly;
        this.dbConfig = dbConfig;

        // For variable-length, just use max buffer size directly
        final int writeBufferSize = dbConfig.getMaxBufferSize();
        byteBuffer = ByteBuffer.allocate(writeBufferSize);
    }


    int capacity() {
        return byteBuffer.capacity();
    }

    int getWriteBufferSize() {
        return byteBuffer.capacity();
    }

    int flush(final OutputStream out) throws IOException {
        if (readOnly) {
            throw new ReadOnlyBufferException("Initialised in read only mode!");
        }

        // If buffer is empty, nothing to flush
        if (byteBuffer.position() == 0) {
            return 0;
        }

        // Write all buffer contents directly to output stream
        final int bytesToWrite = byteBuffer.position();
        out.write(byteBuffer.array(), 0, bytesToWrite);
        out.flush();

        return bytesToWrite;
    }

    void readFromFiles(List<RandomAccessFile> files,
            final boolean reverse, final Consumer<ByteBuffer> recordConsumer) throws IOException {
        for (RandomAccessFile file : files) {
            readFromFile(file, reverse, recordConsumer);
        }
    }
    /**
     * Read variable-length records from a file, supporting both forward and backward iteration.
     * No longer depends on fixed block sizes - works directly with variable-length records.
     */
    void readFromFile(final RandomAccessFile file, final boolean reverse,
                      final Consumer<ByteBuffer> recordConsumer) throws IOException {

        if (reverse) {
            // This is needed for compaction: newer records (at end) should overwrite older ones
            while (file.getFilePointer() != 0) {
                byteBuffer.clear();

                final long currentPosition = file.getFilePointer();
                final long bytesToRead = Math.min(currentPosition, byteBuffer.capacity());
                final long seekPosition = currentPosition - bytesToRead;

                // Seek to the start position for this chunk
                file.seek(seekPosition);

                // Read the chunk and process variable-length records
                fillBuffer(file, recordConsumer, true);

                // Move file pointer back for next iteration
                file.seek(seekPosition);
            }
        } else {
            // Read file forwards until end
            // This is used for normal iteration and recovery
            while (true) {
                byteBuffer.clear();
                final int bytesRead = fillBuffer(file, recordConsumer, false);

                // Stop when we reach end of file (no more data to read)
                if (bytesRead == 0) {
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

    byte[] array() {
        return byteBuffer.array();
    }

    boolean isDirty() {
        return byteBuffer.position() > 0;
    }

    boolean isFull() {
        return byteBuffer.remaining() == 0; // Perfect alignment, so this works.
    }

    int add(long key, byte[] value, int valueOffset, int valueLength) {
        if (readOnly) {
            throw new ReadOnlyBufferException("Initialised in read only mode!");
        }

        Config.validateValueLength(valueLength);

        // Check if we have space for this record
        int recordSize = Config.calculateRecordSize(valueLength);
        if (byteBuffer.remaining() < recordSize) {
            throw new BufferFullException(recordSize, byteBuffer.remaining());
        }

        final int address = byteBuffer.position();

        // Write variable-length record: [Key:8][Length:4][Value:variable][Length:4]
        byteBuffer.putLong(key);                                    // Key (8 bytes)
        byteBuffer.putInt(valueLength);                             // Length header (4 bytes)
        byteBuffer.put(value, valueOffset, valueLength);           // Value (variable bytes)
        byteBuffer.putInt(valueLength);                             // Length footer (4 bytes)

        return address;
    }
    int add(long key, byte[] value, int valueOffset) {
        return add(key, value, valueOffset, value.length - valueOffset);
    }


    /**
     * Attempts to update a key in the in-memory buffer after verifying the key.
     *
     * @param key             The key to be updated
     * @param newValue        The byte array containing the new value
     * @param valueOffset     The offset in the value byte array
     * @param addressInBuffer The address in the buffer at which the key value pair exists
     * @return true if the update succeeds after key verification, false otherwise
     */
    boolean update(long key, byte[] newValue, int valueOffset, int addressInBuffer) {
        long savedKey = byteBuffer.getLong(addressInBuffer);
        if (savedKey != key) {
            return false;
        }
        System.arraycopy(newValue, valueOffset, byteBuffer.array(), addressInBuffer + KEY_SIZE, valueSize);
        return true;
    }

    /**
     * Always call this from a synchronised context, since it will provide a snapshot of data in the
     * current buffer.
     */
    Enumeration<ByteBuffer> iterator(final boolean reverse) {
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

    void clear() {
        byteBuffer = ByteBuffer.allocate(byteBuffer.capacity());
    }
}
