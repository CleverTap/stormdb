package com.clevertap.stormdb;

import com.clevertap.stormdb.exceptions.InconsistentDataException;
import com.clevertap.stormdb.exceptions.ReservedKeyException;
import com.clevertap.stormdb.exceptions.StormDBException;
import com.clevertap.stormdb.exceptions.StormDBRuntimeException;
import com.clevertap.stormdb.utils.ByteUtil;
import com.clevertap.stormdb.utils.RecordUtil;
import gnu.trove.map.hash.TIntIntHashMap;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static final int RECORDS_PER_BLOCK = 128;
    public static final int CRC_SIZE = 4; // CRC32.
    // TODO: 13/07/20 Make it configurable
    private static final long COMPACTION_WAIT_TIMEOUT_MS = (long) 1000 * 60; // 1 minute for now.
    private static final long BUFFER_FLUSH_TIMEOUT_MS = (long) 1000 * 60; // 1 minute for now.
    public static final int MIN_BUFFERS_TO_COMPACT = 8;
    public static final int DATA_TO_WAL_FILE_RATIO = 10;

    protected static final int RESERVED_KEY_MARKER = 0xffffffff;
    private static final int NO_MAPPING_FOUND = 0xffffffff;

    protected static final int KEY_SIZE = 4;

    private static final String FILE_NAME_DATA = "data";
    private static final String FILE_NAME_WAL = "wal";
    private static final String FILE_TYPE_NEXT = ".next";
    private static final String FILE_TYPE_DELETE = ".del";

    /**
     * Key: The actual key within this KV store.
     * <p>
     * Value: The offset (either in the data file, or in the WAL file)
     * <p>
     * Note: Negative offset indicates an offset in the
     */
    // TODO: 03/07/2020 change this map to one that is array based (saves 1/2 the size)
    private final TIntIntHashMap index = new TIntIntHashMap(100_000, 0.95f, Integer.MAX_VALUE,
            NO_MAPPING_FOUND);
    // TODO: 08/07/20 Revisit bitset memory optimization later.
    private BitSet dataInWalFile = new BitSet();

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    // TODO: 16/07/20 Make sure Threadlocal when GC'd closes open file handles.
    private final ThreadLocal<RandomAccessFileWrapper> walReader = new ThreadLocal<>();
    private final ThreadLocal<RandomAccessFileWrapper> walNextReader = new ThreadLocal<>();
    private final ThreadLocal<RandomAccessFileWrapper> dataReader = new ThreadLocal<>();
    private final ThreadLocal<RandomAccessFileWrapper> dataNextReader = new ThreadLocal<>();

    private final Buffer buffer;
    private long lastBufferFlushTimeMs;

    private final int valueSize;
    private final int recordSize;

    private long bytesInWalFile = -1; // Will be initialised on the first write.
    private final File dbDirFile;
    private boolean autoCompact;
    private CompactionState compactionObject;

    private File dataFile;
    private File walFile;

    private DataOutputStream walOut;

    private Thread tWorker;
    private Object compactionSync = new Object();
    private final Object compactionLock = new Object();
    private boolean shutDown = false;
    private boolean useExecutorService = false; // We need this flag if using ES is done mid-way.

    private static final Logger LOG = LoggerFactory.getLogger(StormDB.class);

    // Common executor service stuff
    private static ExecutorService executorService;
    private static final ArrayList<StormDB> instancesServed = new ArrayList<>();
    private final static Object commonCompactionSync = new Object();
    private static boolean esShutDown = false;

    public StormDB(int valueSize, String dbDir) throws IOException {
        this(valueSize, dbDir, true);
    }

    public StormDB(final int valueSize, final String dbDir, final boolean autoCompact)
            throws IOException {
        this.valueSize = valueSize;
        dbDirFile = new File(dbDir);
        this.autoCompact = autoCompact;
        //noinspection ResultOfMethodCallIgnored
        dbDirFile.mkdirs();

        recordSize = valueSize + KEY_SIZE;

        buffer = new Buffer(valueSize, false);
        lastBufferFlushTimeMs = System.currentTimeMillis();

        dataFile = new File(dbDirFile.getAbsolutePath() + File.separator + FILE_NAME_DATA);
        walFile = new File(dbDirFile.getAbsolutePath() + File.separator + FILE_NAME_WAL);

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
        } else {
            // New database. Write value size to the meta.
            final ByteBuffer out = ByteBuffer.allocate(4);
            out.putInt(valueSize);
            Files.write(metaFile.toPath(), out.array());
        }

        initWalOut();

        recover();
        buildIndex();

        setupWorkerThread();
    }

    public static void initExecutorService(int nThreads) {
        // We will create 1 extra thread to accommodate the poll/wait thread
        StormDB.executorService = Executors.newFixedThreadPool(nThreads + 1);
        executorService.submit(() -> {
            while (!esShutDown) {
                try {
                    synchronized (commonCompactionSync) {
                        commonCompactionSync.wait(COMPACTION_WAIT_TIMEOUT_MS);
                    }
                    synchronized (instancesServed) {
                        for (StormDB stormDB : instancesServed) {
                            if (stormDB.autoCompact && stormDB.shouldCompact()) {
                                LOG.info("Auto Compacting now.");
                                executorService.submit(() -> {
                                    try {
                                        stormDB.compact();
                                    } catch (IOException e) {
                                        LOG.error("IOException while compacting - " + e.getMessage());
                                    }
                                });
                            } else if (stormDB.shouldFlushBuffer()) {
                                LOG.info("Flushing buffer to disk on timeout.");
                                stormDB.flush();
                            }
                        }
                    }
                } catch (InterruptedException | IOException e) { // NOSONAR - there's nothing else that we can do.
                    LOG.error("Compaction failure!", e);
                }
            }
        });
    }

    private void initWalOut() throws FileNotFoundException {
        walOut = new DataOutputStream(new FileOutputStream(walFile, true));
        bytesInWalFile = walFile.length();
    }

    // TODO: 16/07/20 We cant potentially have 1000 threads if those many instances are open.
    // Add support for external executor service.
    private void setupWorkerThread() {
        if(executorService == null) {
            tWorker = new Thread(() -> {
                while (!shutDown) {
                    try {
                        synchronized (compactionSync) {
                            compactionSync.wait(COMPACTION_WAIT_TIMEOUT_MS);
                        }
                        if (autoCompact && shouldCompact()) {
                            LOG.info("Auto Compacting now.");
                            compact();
                        } else if (shouldFlushBuffer()) {
                            LOG.info("Flushing buffer to disk on timeout.");
                            flush();
                        }
                    } catch (InterruptedException | IOException e) { // NOSONAR - there's nothing else that we can do.
                        LOG.error("Compaction failure!", e);
                    }
                }
            });
            tWorker.start();
        } else {
            useExecutorService = true;
            synchronized (instancesServed) {
                instancesServed.add(this);
            }
            compactionSync = commonCompactionSync;
        }
    }

    private boolean shouldFlushBuffer() {
        if (System.currentTimeMillis() - lastBufferFlushTimeMs > BUFFER_FLUSH_TIMEOUT_MS) {
            return true;
        }
        return false;
    }

    private boolean shouldCompact() {
        rwLock.readLock().lock();
        try {
            if (isWalFileBigEnough(
                    isCompactionInProgress() ? compactionObject.nextWalFile : walFile)) {
                return true;
            }
        } finally {
            rwLock.readLock().unlock();
        }
        return false;
    }

    private boolean isWalFileBigEnough(File walFile) {
        if (walFile.exists()) {
            final long walLength = walFile.length();
            if (walLength >= MIN_BUFFERS_TO_COMPACT * buffer.capacity()) {
                if (!dataFile.exists()) {
                    return true;
                } else {
                    // We should compare with data file irrespective of whether compaction is in progress
                    // It will be an aprox measure during compaction, but we will have to live with that.
                    if (walFile.length() * DATA_TO_WAL_FILE_RATIO >= dataFile.length()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void buildIndex() throws IOException {
        rwLock.readLock().lock();
        try {
            // Iterating data first ensures bitsets are not needed.
            LOG.info("Building index for the data file.");
            buildIndexFromFile(false);
            LOG.info("Building index for the wal file.");
            buildIndexFromFile(true);
            LOG.info("Finished building index.");
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private void buildIndexFromFile(boolean isWal) throws IOException {
        final Buffer reader = new Buffer(valueSize, true);

        // First figure right file to read.
        File file;
        ThreadLocal<RandomAccessFileWrapper> fileReader;
        if(isWal) {
            file = walFile;
            fileReader = walReader;
        } else {
            file = dataFile;
            fileReader = dataReader;
        }

        if (file.exists()) {
            final RandomAccessFile walFileReader = getReadRandomAccessFile(fileReader, file);
            final int[] fileIndex = {0};
            // Always iterate forward even for wal. In case of wal, entries are overwritten.
            // A small price to pay for not needing bitsets.
            reader.readFromFile(walFileReader, false, entry -> {
                final int key = entry.getInt();
                index.put(key, fileIndex[0]++);
                if(isWal) {
                    dataInWalFile.set(key);
                }
            });
        }
    }

    /**
     * Recovers the database if it's corrupted.
     * <p>
     * Calling this brings the database to a state where exactly two files exist: the WAL file, and
     * the data file.
     */
    private void recover() throws IOException {
        // If the database was shutdown during a compaction, delete data.next,
        // and append wal.next to wal.
        final File nextWalFile = new File(dbDirFile.getAbsolutePath()
                + File.separator + FILE_NAME_WAL + FILE_TYPE_NEXT);

        boolean nextWalFileDeleted = false;

        if (nextWalFile.exists()) {
            // Safe, since walOut is always opened in an append only mode.
            Files.copy(nextWalFile.toPath(), walOut);
            walOut.flush();
            Files.delete(nextWalFile.toPath());
            nextWalFileDeleted = true;
        }

        // If a next data file exists, but no corresponding nextWalFile, then that probably
        // means that just towards the end of the last compaction, the nextWalFile was deleted,
        // but the rename of next data file failed. Don't delete, but simply treat the next data
        // as a part of the WAL file.
        final File nextDataFile = new File(dbDirFile.getAbsolutePath() + File.separator +
                FILE_NAME_DATA + FILE_TYPE_NEXT);

        if (nextDataFile.exists() && !nextWalFileDeleted) {
            // Safe, since walOut is always opened in an append only mode.
            Files.copy(nextDataFile.toPath(), walOut);
            walOut.flush();

            Files.delete(nextDataFile.toPath());
        }

        // Let's run a sequential scan and verify the two files.
        {
            final File verifiedWalFile = BlockUtil.verifyBlocks(walFile, valueSize);
            if (verifiedWalFile != walFile) {
                walFile = verifiedWalFile;
                initWalOut();
            }
        }

        dataFile = BlockUtil.verifyBlocks(dataFile, valueSize);
    }

    // TODO: 16/07/20 Look at https://docs.oracle.com/javase/tutorial/essential/io/fileOps.html#atomic
    private void rename(final File file, final File destination) throws IOException {
        if (file.exists() && !file.renameTo(destination)) {
            throw new IOException("Failed to rename " + file.getAbsolutePath()
                    + " to " + destination.getAbsolutePath());
        }
    }

    /**
     * This should always be called from synchronized context.
     *
     * @return If compaction is in progress
     */
    private boolean isCompactionInProgress() {
        return compactionObject != null;
    }

    // TODO: 13/07/20 Handle case where compaction takes too long.
    // TODO: 13/07/20 Handle case where compaction thread keeps failing.
    public void compact() throws IOException {
        synchronized (compactionLock) {
            final long start = System.currentTimeMillis();

            // 1. Move wal to wal.prev and create new wal file.
            rwLock.writeLock().lock();
            try {
                // First flush all data.
                // This is because we will be resetting bytesInWalFile below and we need to get all
                // buffer to file so that their offsets are honoured.
                flush();

                LOG.info("Beginning compaction with bytesInWalFile={}", bytesInWalFile);
                // Check whether there was any data coming in. If not simply bail out.
                if (bytesInWalFile == 0) {
                    return;
                }

                compactionObject = new CompactionState();

                compactionObject.nextWalFile = new File(dbDirFile.getAbsolutePath()
                        + File.separator + FILE_NAME_WAL + FILE_TYPE_NEXT);

                // Create new walOut File
                walOut = new DataOutputStream(new FileOutputStream(compactionObject.nextWalFile));
                bytesInWalFile = 0;
                compactionObject.nextFileRecordIndex = 0;

            } finally {
                rwLock.writeLock().unlock();
            }

            // 2. Process wal.current file and out to data.next file.
            // 3. Process data.current file.
            compactionObject.nextDataFile = new File(dbDirFile.getAbsolutePath() + File.separator +
                    FILE_NAME_DATA + FILE_TYPE_NEXT);

            try (final BufferedOutputStream out =
                    new BufferedOutputStream(new FileOutputStream(compactionObject.nextDataFile),
                            buffer.getWriteBufferSize())) {

                final Buffer tmpBuffer = new Buffer(valueSize, false);

                iterate(false, false, (key, data, offset) -> {
                    tmpBuffer.add(key, data, offset);

                    if (tmpBuffer.isFull()) {
                        flushNext(out, tmpBuffer);
                    }
                });

                if (tmpBuffer.isDirty()) {
                    flushNext(out, tmpBuffer);
                }
            }

            File walFileToDelete = new File(dbDirFile.getAbsolutePath() + File.separator +
                    FILE_NAME_WAL + FILE_TYPE_DELETE);
            File dataFileToDelete = new File(dbDirFile.getAbsolutePath() + File.separator +
                    FILE_NAME_DATA + FILE_TYPE_DELETE);

            rwLock.writeLock().lock();
            try {
                // First rename prevWalFile and prevDataFile so that .next can be renamed
                rename(walFile, walFileToDelete);
                rename(dataFile, dataFileToDelete);

                // Now make bitsets point right.
                dataInWalFile = compactionObject.dataInNextWalFile;

                // Create new file references for Thread locals to be aware of.
                // Rename *.next to *.current
                walFile = new File(
                        dbDirFile.getAbsolutePath() + File.separator + FILE_NAME_WAL);
                rename(compactionObject.nextWalFile, walFile);
                dataFile = new File(
                        dbDirFile.getAbsolutePath() + File.separator + FILE_NAME_DATA);
                rename(compactionObject.nextDataFile, dataFile);

                compactionObject = null;
            } finally {
                rwLock.writeLock().unlock();
            }

            // 4. Delete old data and wal
            if (walFileToDelete.exists() && !Files.deleteIfExists(walFileToDelete.toPath())) {
                LOG.error("Unable to delete file {}", walFileToDelete.getName());
            }
            if (dataFileToDelete.exists() && !Files.deleteIfExists(dataFileToDelete.toPath())) {
                LOG.error("Unable to delete file {}", dataFileToDelete.getName());
            }
            LOG.info("Compaction completed successfully in {} ms", System.currentTimeMillis() - start);
        }
    }

    private void flushNext(OutputStream out, Buffer buffer) throws IOException {
        buffer.flush(out);

        try {
            rwLock.writeLock().lock();
            final Enumeration<ByteBuffer> iterator = buffer.iterator(false);

            while (iterator.hasMoreElements()) {
                final ByteBuffer byteBuffer = iterator.nextElement();
                final long address = RecordUtil
                        .indexToAddress(recordSize, compactionObject.nextFileRecordIndex);
                compactionObject.nextFileRecordIndex++;
                final int key = byteBuffer.getInt();
                if (!compactionObject.dataInNextWalFile.get(key)) {
                    index.put(key, RecordUtil.addressToIndex(recordSize, address));
                    compactionObject.dataInNextFile.set(key);
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }

        buffer.clear();
    }

    public void put(final byte[] key, final byte[] value, final int valueOffset)
            throws IOException {
        put(ByteUtil.toInt(key, 0), value, valueOffset);
    }

    public void put(final byte[] key, final byte[] value) throws IOException {
        put(ByteUtil.toInt(key, 0), value);
    }

    public void put(int key, byte[] value) throws IOException {
        put(key, value, 0);
    }

    public void put(int key, byte[] value, int valueOffset) throws IOException {
        if (key == RESERVED_KEY_MARKER) {
            throw new ReservedKeyException(RESERVED_KEY_MARKER);
        }
        rwLock.writeLock().lock();

        try {
            // TODO: 07/07/2020 Optimisation: if the current key is in the write buffer,
            // TODO: 07/07/2020 don't append, but perform an inplace update
            if (buffer.isFull()) {
                flush();
                // Let compaction thread eval if there is a need for compaction.
                // If buffer length is too small, it might result in too many calls.
                synchronized (compactionSync) {
                    compactionSync.notify();
                }
            }

            // Write to the write buffer.
            final int addressInBuffer = buffer.add(key, value, valueOffset);

            final int address = RecordUtil.addressToIndex(recordSize,
                    bytesInWalFile + addressInBuffer);
            index.put(key, address);

            if (isCompactionInProgress()) {
                compactionObject.dataInNextWalFile.set(key);
            } else {
                dataInWalFile.set(key);
            }

        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void flush() throws IOException {
        rwLock.writeLock().lock();
        try {
            // walOut is initialised on the first write to the writeBuffer.
            if (walOut == null || !buffer.isDirty()) {
                return;
            }

            bytesInWalFile += buffer.flush(walOut);
            buffer.clear();

            lastBufferFlushTimeMs = System.currentTimeMillis();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void iterate(final EntryConsumer consumer) throws IOException {
        iterate(true, true, consumer);
    }

    private void iterate(final boolean useLatestWalFile, final boolean readInMemoryBuffer,
            final EntryConsumer consumer) throws IOException {
        final ArrayList<RandomAccessFile> walFiles = new ArrayList<>(2);
        final ArrayList<RandomAccessFile> dataFiles = new ArrayList<>(1);

        Enumeration<ByteBuffer> inMemRecords = null;
        rwLock.readLock().lock();
        try {
            if (isCompactionInProgress() && useLatestWalFile) {
                RandomAccessFile reader = getReadRandomAccessFile(walNextReader,
                        compactionObject.nextWalFile);
                reader.seek(reader.length());
                walFiles.add(reader);
            }

            if (walFile.exists()) {
                RandomAccessFile reader = getReadRandomAccessFile(walReader, walFile);
                reader.seek(walFile.length());
                walFiles.add(reader);
            }

            if (dataFile.exists()) {
                RandomAccessFile reader = getReadRandomAccessFile(walReader, dataFile);
                dataFiles.add(reader);
            }

            if (readInMemoryBuffer) {
                inMemRecords = buffer.iterator(true);
            }
        } finally {
            rwLock.readLock().unlock();
        }

        final BitSet keysRead = new BitSet(index.size());

        final Consumer<ByteBuffer> entryConsumer = entry -> {
            final int key = entry.getInt();
            // TODO: 08/07/2020 if we need to support the whole range of 4 billion keys, we should use a long as the bitset is +ve
            final boolean b = keysRead.get(key);
            if (!b) {
                try {
                    consumer.accept(key, entry.array(), entry.position());
                } catch (IOException e) {
                    throw new StormDBRuntimeException(e);
                }
                keysRead.set(key);
            }
        };

        if (readInMemoryBuffer) {
            while (inMemRecords.hasMoreElements()) {
                final ByteBuffer entry = inMemRecords.nextElement();
                entryConsumer.accept(entry);
            }
        }

        final Buffer reader = new Buffer(valueSize, true);
        reader.readFromFiles(walFiles, true, entryConsumer);
        reader.readFromFiles(dataFiles, false, entryConsumer);
    }

    public byte[] randomGet(final int key) throws IOException, StormDBException {
        int recordIndex;
        final RandomAccessFile f;
        byte[] value;
        rwLock.readLock().lock();
        final long address;
        try {
            recordIndex = index.get(key);
            if (recordIndex == NO_MAPPING_FOUND) { // No mapping value.
                return null; // NOSONAR - returning null is a part of the interface.
            }

            value = new byte[valueSize];

            if (isCompactionInProgress() && compactionObject.dataInNextWalFile.get(key)) {
                address = RecordUtil.indexToAddress(recordSize, recordIndex);
                if (address >= bytesInWalFile) {
                    System.arraycopy(buffer.array(), (int) (address - bytesInWalFile + KEY_SIZE),
                            value, 0, valueSize);
                    return value;
                }
                f = getReadRandomAccessFile(walNextReader, compactionObject.nextWalFile);
            } else if (isCompactionInProgress() && compactionObject.dataInNextFile.get(key)) {
                address = RecordUtil.indexToAddress(recordSize, recordIndex);
                f = getReadRandomAccessFile(dataNextReader, compactionObject.nextDataFile);
            } else if (dataInWalFile.get(key)) {
                address = RecordUtil.indexToAddress(recordSize, recordIndex);
                // If compaction is in progress, we can not read in-memory.
                if (!isCompactionInProgress() && address >= bytesInWalFile) {
                    System.arraycopy(buffer.array(), (int) (address - bytesInWalFile + KEY_SIZE),
                            value, 0, valueSize);
                    return value;
                }
                f = getReadRandomAccessFile(walReader, walFile);
            } else {
                address = RecordUtil.indexToAddress(recordSize, recordIndex);
                f = getReadRandomAccessFile(dataReader, dataFile);
            }
        } finally {
            rwLock.readLock().unlock();
        }

        f.seek(address);
        if (f.readInt() != key) {
            throw new InconsistentDataException();
        }
        final int bytesRead = f.read(value);
        if (bytesRead != valueSize) {
            // TODO: 03/07/2020 perhaps it's more appropriate to return null (record lost)
            throw new StormDBException("Possible data corruption detected!");
        }
        return value;
    }

    private static RandomAccessFileWrapper getReadRandomAccessFile(
            ThreadLocal<RandomAccessFileWrapper> reader,
            File file) throws IOException {
        RandomAccessFileWrapper f = reader.get();
        if (f == null || !f.isSameFile(file)) {
            // TODO: 16/07/20 Check if we need to call f.close(); for older handle
            f = new RandomAccessFileWrapper(file, "r");
            reader.set(f);
        }
        return f;
    }

    public void close() throws IOException, InterruptedException {
        flush();
        shutDown = true;
        if(useExecutorService) {
            synchronized (instancesServed) {
                instancesServed.remove(this);
            }
        } else {
            synchronized (compactionSync) {
                compactionSync.notifyAll();
            }
            tWorker.join();
        }
    }

    public static void shutDownExecutorService() throws InterruptedException {
        if(executorService != null) {
            esShutDown = true;
            synchronized (commonCompactionSync) {
                commonCompactionSync.notifyAll();
            }
            executorService.shutdown();
            if (!executorService.awaitTermination(5 * 60, TimeUnit.SECONDS)) {
                LOG.error("Unable to shutdown StormDB executor service in 5 minutes.");
            }
            executorService = null;
        }
    }

}
