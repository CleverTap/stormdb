package com.clevertap.stormdb;

import gnu.trove.map.hash.TIntIntHashMap;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.BitSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * Protocol: key (4 bytes) | value (fixed bytes).
 * <p>
 * Notes:
 * <ul>
 *     <li>0xffffffff is reserved for internal structures (used as the sync marker)</li>
 * </ul>
 * <p>
 */
public class StormDB {

    private static final String FILE_NAME_DATA = "data";
    private static final String FILE_NAME_WAL = "wal";

    private static final int OFFSET_MASK = 0x7fffffff;
    private static final int OFFSET_IN_WAL = 0x80000000;
    protected static final int FOUR_MB = 4 * 1024 * 1024;

    /**
     * Key: The actual key within this KV store.
     * <p>
     * Value: The offset (either in the data file, or in the WAL file)
     * <p>
     * Note: Negative offset indicates an offset in the
     */
    // TODO: 03/07/2020 change this map to one that is array based (saves 1/2 the size)
    private final TIntIntHashMap index = new TIntIntHashMap(10_000_000, 0.95f, Integer.MAX_VALUE,
            Integer.MAX_VALUE);

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final ThreadLocal<RandomAccessFile> walReaders = new ThreadLocal<>();
    private final ThreadLocal<RandomAccessFile> dataReaders = new ThreadLocal<>();


    /**
     * Align to the nearest 4096 block, based on the size of the value. This improves sequential
     * reads by reading data in bulk. Based on previous performance tests, reading just {@link
     * #valueSize} bytes at a time is slower.
     */
    private final int blockSize;

    private final ByteBuffer writeBuffer;

    private final int valueSize;
    private final int recordSize;
    private int writeOffsetWal = -1; // Will be initialised on the first write.
    private final File dbDirFile;

    private File dataFile;
    private File walFile;

    private DataOutputStream walOut;

    public StormDB(final int valueSize, final String dbDir) throws IOException {
        this.valueSize = valueSize;
        dbDirFile = new File(dbDir);
        //noinspection ResultOfMethodCallIgnored
        dbDirFile.mkdirs();

        recordSize = valueSize + 4; // +4 for the key.

        blockSize = (4096 / recordSize) * recordSize; // +4 for the key
        writeBuffer = ByteBuffer.allocate((FOUR_MB / recordSize) * recordSize);

        dataFile = new File(dbDirFile.getAbsolutePath() + "/" + FILE_NAME_DATA);
        walFile = new File(dbDirFile.getAbsolutePath() + "/" + FILE_NAME_WAL);

        // Open DB.
        final File metaFile = new File(dbDir + "/meta");
        if (metaFile.exists()) {
            // Ensure that the valueSize has not changed.
            final byte[] bytes = Files.readAllBytes(metaFile.toPath());
            final ByteBuffer meta = ByteBuffer.wrap(bytes);
            final int valueSizeFromMeta = meta.getInt();
            if (valueSizeFromMeta != valueSize) {
                throw new IOException("The path " + dbDir
                        + " contains a StormDB database with the value size "
                        + valueSizeFromMeta + " bytes. "
                        + "However, " + valueSize + " bytes was provided!");
            }
            buildIndex(false);
            buildIndex(true);
        } else {
            // New database. Write value size to the meta.
            final ByteBuffer out = ByteBuffer.allocate(4);
            out.putInt(valueSize);
            Files.write(metaFile.toPath(), out.array());
        }

        walOut = new DataOutputStream(new FileOutputStream(walFile, true));
        writeOffsetWal = (int) (walFile.length() / recordSize);

        if (walFile.length() % recordSize != 0) {
            // Corrupted WAL - somebody should run compact!
            throw new IOException("WAL file corrupted! Compact DB before writing again!");
        }
    }

    public void put(int key, byte[] value) throws IOException {
        put(key, value, 0);
    }

    public void put(int key, byte[] value, int valueOffset) throws IOException {
        if (key == Integer.MAX_VALUE) {
            throw new RuntimeException("Key " + Integer.MAX_VALUE
                    + " is a reserved key (used for internal computation)");
        }
        final WriteLock lock = rwLock.writeLock();

        int offset;

        lock.lock();

        try {
            // TODO: 07/07/2020 Optimisation: if the current key is in the write buffer,
            // TODO: 07/07/2020 don't append, but perform an inplace update

            if (writeBuffer.remaining() == 0) {
                flush();
            }

            // Write to the write buffer.
            writeBuffer.putInt(key);
            writeBuffer.put(value, valueOffset, valueSize);

            offset = writeOffsetWal + (writeBuffer.position() - recordSize) / recordSize;
        } finally {
            lock.unlock();
        }

        index.put(key, getIndex(true, offset));
    }

    private int getIndex(final boolean wal, final int offset) {
        if (wal) {
            return offset | OFFSET_IN_WAL;
        } else {
            return offset;
        }
    }

    private void flush() throws IOException {
        rwLock.writeLock().lock();
        try {
            // walOut is initialised on the first write to the writeBuffer.
            if (walOut == null || writeBuffer.position() == 0) {
                return;
            }
            final int position = writeBuffer.position();
            walOut.write(writeBuffer.array(), 0, position);
            walOut.flush();
            writeBuffer.clear();
            writeOffsetWal += position / recordSize;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public interface KVConsumer {

        void accept(final int key, final byte[] data, final int offset);
    }

    public void iterate(final KVConsumer consumer) throws IOException {
        final RandomAccessFile walReader, dataReader;

        rwLock.readLock().lock();
        try {
            // TODO: 05/07/2020 Keep a mem buffer since a block needs to be flushed fully for backwards iteration.
            // TODO: 05/07/2020 consider partial writes and alignment
            // TODO: 05/07/2020 best to ensure that the last 4 bytes are the sync bytes
            if (walFile.exists()) {
                walReader = new RandomAccessFile(walFile, "r");
                walReader.seek(walFile.length());
            } else {
                walReader = null;
            }

            if (dataFile.exists()) {
                dataReader = new RandomAccessFile(dataFile, "r");
                dataReader.seek(dataFile.length());
            } else {
                dataReader = null;
            }
        } finally {
            rwLock.readLock().unlock();
        }

        final RandomAccessFile[] files = new RandomAccessFile[]{walReader, dataReader};

        // Always 4 MB, regardless of the value of this.blockSize. Since RandomAccessFile cannot
        // be buffered, we must make a large get request to the underlying native calls.
        final int blockSize = (FOUR_MB / recordSize) * recordSize;

        final ByteBuffer buf = ByteBuffer.allocate(blockSize);

        final BitSet keysRead = new BitSet(index.size());

        for (RandomAccessFile file : files) {
            if (file == null) {
                continue;
            }
            while (file.getFilePointer() != 0) {
                buf.clear();

                final long validBytesRemaining = file.getFilePointer() - blockSize;
                file.seek(Math.max(validBytesRemaining, 0));

                final int bytesRead = file.read(buf.array());

                // Set the position again, since the read op moved the cursor back ahead.
                file.seek(Math.max(validBytesRemaining, 0));

                // Note: There's the possibility that we'll read the head of the file twice,
                // but that's okay, since we iterate in a backwards fashion.
                buf.limit(bytesRead);
                // TODO: 05/07/2020 assert that this is in perfect alignment of 1 KV pair
                buf.position(buf.limit());

                while (buf.position() != 0) {
                    buf.position(buf.position() - recordSize);
                    final int key = buf.getInt();
                    // TODO: 08/07/2020 if we need to support the whole range of 4 billion keys, we should use a long as the bitset is +ve
                    final boolean b = keysRead.get(key);
                    if (!b) {
                        consumer.accept(key, buf.array(), buf.position());
                        keysRead.set(key);
                    }

                    // Do this again, since we read the buffer backwards too.
                    // -4 because we read the key only.
                    buf.position(buf.position() - 4);
                }
            }
        }
    }

    public byte[] randomGet(final int key) throws IOException {
        final int offset;
        rwLock.readLock().lock();
        try {
            offset = index.get(key);
        } finally {
            rwLock.readLock().unlock();
        }

        final int offsetInData = offset & OFFSET_MASK;

        if (offset == Integer.MAX_VALUE) {
            return null;
        }

        final boolean valueInWal = (offset & OFFSET_IN_WAL) == OFFSET_IN_WAL;
        final long position = offsetInData * (long) recordSize;

        ThreadLocal<RandomAccessFile> th = valueInWal ? walReaders : dataReaders;

        RandomAccessFile f = th.get();
        if (f == null) {
            f = new RandomAccessFile(valueInWal ? walFile : dataFile, "r");
            th.set(f);
        }

        // Check if the key is present in the writeBuffer.
        final byte[] value = new byte[valueSize];
        rwLock.readLock().lock();
        try {
            if (offsetInData >= writeOffsetWal) {
                final int offsetInWriteBuffer = (offsetInData - writeOffsetWal) * recordSize;
                System.arraycopy(writeBuffer.array(), offsetInWriteBuffer + 4, value, 0, valueSize);
                return value;
            }
        } finally {
            rwLock.readLock().unlock();
        }

        f.seek(position + 4); // +4 for the key.
        final int bytesRead = f.read(value);
        if (bytesRead != valueSize) {
            // TODO: 03/07/2020 perhaps it's more appropriate to return null (record lost)
            throw new IOException("Corrupted");
        }
        return value;
    }

    /**
     * Builds a key to offset index by reading the following: 1. Data file (as a result of the last
     * compaction) 2. WAL file (contains the most recently written records)
     * <p>
     * Both files are iterated over sequentially.
     */
    private void buildIndex(final boolean walContext) throws IOException {
        final File dataFile = walContext ? this.walFile : this.dataFile;
        if (!dataFile.exists()) {
            return;
        }
        final BufferedInputStream bufIn = new BufferedInputStream(new FileInputStream(dataFile));
        final DataInputStream in = new DataInputStream(bufIn);

        final ByteBuffer buf = ByteBuffer.allocate(blockSize);

        int dataFileOffset = 0;

        while (true) {
            buf.clear();

            final int limit = in.read(buf.array());
            if (limit == -1) {
                break;
            }
            buf.limit(limit);

            while (buf.remaining() >= recordSize) {
                // TODO: 07/07/2020 assert alignment
                final int key = buf.getInt();
                buf.position(buf.position() + valueSize);
                index.put(key, getIndex(walContext, dataFileOffset));

                dataFileOffset++;
            }
        }
    }

    public void close() throws IOException {
        flush();
    }
}
