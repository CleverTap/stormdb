package com.clevertap.stormdb;

import com.clevertap.stormdb.exceptions.InconsistentDataException;
import com.clevertap.stormdb.exceptions.IncorrectConfigException;
import com.clevertap.stormdb.exceptions.ReservedKeyException;
import com.clevertap.stormdb.exceptions.StormDBException;
import com.clevertap.stormdb.exceptions.StormDBRuntimeException;
import com.clevertap.stormdb.internal.RandomAccessFilePool;
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
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.List;
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

    static final int RESERVED_KEY_MARKER = 0xffffffff;
    private static final int NO_MAPPING_FOUND = 0xffffffff;

    private static final String FILE_NAME_DATA = "data";
    private static final String FILE_NAME_WAL = "wal";
    private static final String FILE_TYPE_NEXT = ".next";

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

    private BitSet dataInWalFile = new BitSet();

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final RandomAccessFilePool filePool;

    private final Buffer buffer;
    private long lastBufferFlushTimeMs;

    private final int recordSize;

    private long bytesInWalFile = -1; // Will be initialised on the first write.
    private final File dbDirFile;
    private final Config conf;
    CompactionState compactionState;

    private File dataFile;
    private File walFile;

    private DataOutputStream walOut;

    private Thread tWorker;
    private final Object compactionSync;
    private final Object compactionLock = new Object();
    private boolean shutDown = false;
    private boolean useExecutorService = false; // We need this flag if using ES is done mid-way.

    Throwable exceptionDuringBackgroundOps = null;

    private static final Logger LOG = LoggerFactory.getLogger(StormDB.class);

    // Common executor service stuff
    private static ExecutorService executorService;
    private static final ArrayList<StormDB> instancesServed = new ArrayList<>();
    private static final Object commonCompactionSync = new Object();
    private static boolean esShutDown = false;

    StormDB(final Config config) throws IOException {
        conf = config;
        dbDirFile = new File(conf.getDbDir());
        //noinspection ResultOfMethodCallIgnored
        dbDirFile.mkdirs();

        recordSize = conf.getValueSize() + Config.KEY_SIZE;

        buffer = new Buffer(conf, false);
        lastBufferFlushTimeMs = System.currentTimeMillis();

        filePool = new RandomAccessFilePool(conf.getOpenFDCount());

        dataFile = new File(dbDirFile.getAbsolutePath() + File.separator + FILE_NAME_DATA);
        walFile = new File(dbDirFile.getAbsolutePath() + File.separator + FILE_NAME_WAL);

        // Open DB.
        final File metaFile = new File(conf.getDbDir() + "/meta");
        if (metaFile.exists()) {
            // Ensure that the valueSize has not changed.
            final byte[] bytes = Files.readAllBytes(metaFile.toPath());
            final ByteBuffer meta = ByteBuffer.wrap(bytes);
            final int valueSizeFromMeta = meta.getInt();
            if (valueSizeFromMeta != conf.getValueSize()) {
                throw new IncorrectConfigException("The path " + conf.getDbDir()
                        + " contains a StormDB database with the value size "
                        + valueSizeFromMeta + " bytes. "
                        + "However, " + conf.getValueSize() + " bytes was provided!");
            }
        } else {
            // New database. Write value size to the meta.
            final ByteBuffer out = ByteBuffer.allocate(4);
            out.putInt(conf.getValueSize());
            Files.write(metaFile.toPath(), out.array());
        }

        initWalOut();

        recover();
        buildIndex();

        if (executorService == null) {
            compactionSync = new Object();
            tWorker = new Thread(() -> {
                while (!shutDown) {
                    try {
                        synchronized (compactionSync) {
                            compactionSync.wait(conf.getCompactionWaitTimeoutMs());
                        }
                        if (conf.autoCompactEnabled() && shouldCompact()) {
                            LOG.info("Auto Compacting now.");
                            compact();
                        } else if (shouldFlushBuffer()) {
                            LOG.info("Flushing buffer to disk on timeout.");
                            flush();
                        }
                    } catch (Throwable e) { // NOSONAR - there's nothing else that we can do.
                        LOG.error("Compaction failure!", e);
                        exceptionDuringBackgroundOps = e;
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

    public static void initExecutorService(int nThreads) {
        // We will create 1 extra thread to accommodate the poll/wait thread
        StormDB.executorService = Executors.newFixedThreadPool(nThreads + 1);
        executorService.submit(() -> {
            esShutDown = false;
            while (!esShutDown) {
                synchronized (commonCompactionSync) {
                    try {
                        commonCompactionSync
                                .wait(Config.getDefaultCompactionWaitTimeoutMs());
                    } catch (InterruptedException e) {
                        // Ignore this one.
                        LOG.error("Interrupted while waiting for the "
                                + "common compaction sync lock", e);

                        synchronized (instancesServed) {
                            for (StormDB stormDB : instancesServed) {
                                stormDB.exceptionDuringBackgroundOps = e;
                            }
                        }

                        executorService.shutdown(); // So that no more databases can be added.
                        // Don't swallow the interruption: https://www.ibm.com/developerworks/java/library/j-jtp05236/index.html?ca=drs-#2.1
                        Thread.currentThread().interrupt();
                    }
                }
                synchronized (instancesServed) {
                    for (StormDB stormDB : instancesServed) {
                        try {
                            if (stormDB.conf.autoCompactEnabled() && stormDB.shouldCompact()) {
                                LOG.info("Auto Compacting now.");
                                executorService.submit(() -> {
                                    try {
                                        stormDB.compact();
                                    } catch (IOException e) {
                                        stormDB.exceptionDuringBackgroundOps = e;
                                        LOG.error("Failed to compact!", e);
                                    }
                                });
                            } else if (stormDB.shouldFlushBuffer()) {
                                LOG.info("Flushing buffer to disk on timeout.");
                                stormDB.flush();
                            }
                        } catch (IOException e) {
                            stormDB.exceptionDuringBackgroundOps = e;
                            LOG.error("Failed to flush an open buffer!", e);
                        }
                    }
                }
            }
        });
    }

    private void initWalOut() throws FileNotFoundException {
        walOut = new DataOutputStream(new FileOutputStream(walFile, true));
        bytesInWalFile = walFile.length();
    }

    private boolean shouldFlushBuffer() {
        return System.currentTimeMillis() - lastBufferFlushTimeMs > conf
                .getBufferFlushTimeoutMs();
    }

    private boolean shouldCompact() {
        rwLock.readLock().lock();
        try {
            if (isWalFileBigEnough(
                    isCompactionInProgress() ? compactionState.nextWalFile : walFile)) {
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
            if (walLength >= conf.getMinBuffersToCompact() * buffer.capacity()) {
                if (!dataFile.exists()) {
                    return true;
                } else {
                    // We should compare with data file irrespective of whether compaction is in progress
                    // It will be an aprox measure during compaction, but we will have to live with that.
                    return walFile.length() * conf.getDataToWalFileRatio() >= dataFile.length();
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
        final Buffer reader = new Buffer(conf, true);

        // First figure right file to read.
        File file = isWal ? walFile : dataFile;

        if (file.exists()) {
            final RandomAccessFileWrapper wrapper = filePool.borrowObject(file);

            final int[] fileIndex = {0};
            // Always iterate forward even for wal. In case of wal, entries are overwritten.
            // A small price to pay for not needing bitsets.
            try {
                reader.readFromFile(wrapper, false, entry -> {
                    final int key = entry.getInt();
                    index.put(key, fileIndex[0]++);
                    if (isWal) {
                        dataInWalFile.set(key);
                    }
                });
            } finally {
                filePool.returnObject(file, wrapper);
            }
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
            initWalOut(); // Will update the byte count.
            Files.delete(nextWalFile.toPath());
            nextWalFileDeleted = true;
        }

        // If a next data file exists, but no corresponding nextWalFile, then it
        // means that just towards the end of the last compaction, the nextWalFile was deleted,
        // but the rename of next data file failed. Don't delete, but simply treat the next data
        // as a part of the WAL file.
        final File nextDataFile = new File(dbDirFile.getAbsolutePath() + File.separator +
                FILE_NAME_DATA + FILE_TYPE_NEXT);

        if (nextDataFile.exists() && !nextWalFileDeleted) {
            // Safe, since walOut is always opened in an append only mode.
            Files.copy(nextDataFile.toPath(), walOut);
            walOut.flush();
            initWalOut(); // Will update the byte count.

            Files.delete(nextDataFile.toPath());
        }

        // Let's run a sequential scan and verify the two files.
        {
            final File verifiedWalFile = BlockUtil.verifyBlocks(walFile, conf.getValueSize());
            if (verifiedWalFile != walFile) {
                walFile = verifiedWalFile;
                initWalOut();
            }
        }

        dataFile = BlockUtil.verifyBlocks(dataFile, conf.getValueSize());
    }

    /**
     * Moves a file atomically.
     *
     * @return A reference to the destination
     */
    private Path move(final File file, final File destination) throws IOException {
        return Files.move(file.toPath(), destination.toPath(),
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * This should always be called from synchronized context.
     *
     * @return If compaction is in progress
     */
    private boolean isCompactionInProgress() {
        return compactionState != null;
    }

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

                compactionState = new CompactionState();

                compactionState.nextWalFile = new File(dbDirFile.getAbsolutePath()
                        + File.separator + FILE_NAME_WAL + FILE_TYPE_NEXT);

                // Create new walOut File
                walOut = new DataOutputStream(new FileOutputStream(compactionState.nextWalFile));
                bytesInWalFile = 0;
                compactionState.nextFileRecordIndex = 0;

            } finally {
                rwLock.writeLock().unlock();
            }

            // 2. Process wal.current file and out to data.next file.
            // 3. Process data.current file.
            compactionState.nextDataFile = new File(dbDirFile.getAbsolutePath() + File.separator +
                    FILE_NAME_DATA + FILE_TYPE_NEXT);

            try (final BufferedOutputStream out =
                    new BufferedOutputStream(new FileOutputStream(compactionState.nextDataFile),
                            buffer.getWriteBufferSize())) {

                final Buffer tmpBuffer = new Buffer(conf, false);

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

            rwLock.writeLock().lock();
            try {
                // First rename prevWalFile and prevDataFile so that .next can be renamed
                walFile = move(compactionState.nextWalFile, walFile).toFile();
                dataFile = move(compactionState.nextDataFile, dataFile).toFile();

                // Now make bitsets point right.
                dataInWalFile = compactionState.dataInNextWalFile;

                compactionState = null;
                filePool.clear();
            } finally {
                rwLock.writeLock().unlock();
            }

            LOG.info("Compaction completed successfully in {} ms",
                    System.currentTimeMillis() - start);
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
                        .indexToAddress(recordSize, compactionState.nextFileRecordIndex);
                compactionState.nextFileRecordIndex++;
                final int key = byteBuffer.getInt();
                if (!compactionState.dataInNextWalFile.get(key)) {
                    index.put(key, RecordUtil.addressToIndex(recordSize, address));
                    compactionState.dataInNextFile.set(key);
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
        put(key, value, 0);
    }

    public void put(int key, byte[] value) throws IOException {
        put(key, value, 0);
    }

    public void put(int key, byte[] value, int valueOffset) throws IOException {
        if (exceptionDuringBackgroundOps != null) {
            throw new StormDBRuntimeException("Will not accept any further writes since the "
                    + "last compaction resulted in an exception!", exceptionDuringBackgroundOps);
        }

        if (key == RESERVED_KEY_MARKER) {
            throw new ReservedKeyException(RESERVED_KEY_MARKER);
        }
        rwLock.writeLock().lock();

        try {
            if (buffer.isFull()) {
                flush();
                // Let compaction thread eval if there is a need for compaction.
                // If buffer length is too small, it might result in too many calls.
                synchronized (compactionSync) {
                    compactionSync.notifyAll();
                }
            }

            // Write to the write buffer.
            final int addressInBuffer = buffer.add(key, value, valueOffset);

            final int recordIndex = RecordUtil.addressToIndex(recordSize,
                    bytesInWalFile + addressInBuffer);
            index.put(key, recordIndex);

            if (isCompactionInProgress()) {
                compactionState.dataInNextWalFile.set(key);
            } else {
                dataInWalFile.set(key);
            }

        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void flush() throws IOException {
        rwLock.writeLock().lock();
        try {
            // walOut is initialised on the first write to the writeBuffer.
            if (walOut == null || !buffer.isDirty()) {
                return;
            }

            bytesInWalFile += buffer.flush(walOut);
            buffer.clear();

            lastBufferFlushTimeMs = System.currentTimeMillis();

            if (isCompactionInProgress() && compactionState.runningForTooLong()) {
                final long secondsSinceStart =
                        (System.currentTimeMillis() - compactionState.getStart()) / 1000;
                exceptionDuringBackgroundOps = new StormDBRuntimeException(
                        "The last compaction has been running for over " + secondsSinceStart
                                + " seconds!");
            }
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
                final RandomAccessFileWrapper reader = filePool
                        .borrowObject(compactionState.nextWalFile);
                reader.seek(reader.length());
                walFiles.add(reader);
            }

            if (walFile.exists()) {
                final RandomAccessFileWrapper reader = filePool.borrowObject(walFile);
                reader.seek(reader.length());
                walFiles.add(reader);
            }

            if (dataFile.exists()) {
                final RandomAccessFileWrapper reader = filePool.borrowObject(dataFile);
                reader.seek(0);
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

        final Consumer<List<RandomAccessFile>> returnFiles = files -> {
            for (RandomAccessFile file : files) {
                assert file instanceof RandomAccessFileWrapper;
                filePool.returnObject(((RandomAccessFileWrapper) file).getFile(),
                        (RandomAccessFileWrapper) file);
            }
        };

        final Buffer reader = new Buffer(conf, true);
        boolean returnDataFilesEarly = true;
        try {
            reader.readFromFiles(walFiles, true, entryConsumer);
            returnDataFilesEarly = false;
        } finally {
            returnFiles.accept(walFiles);
            if (returnDataFilesEarly) {
                returnFiles.accept(dataFiles);
            }
        }

        try {
            reader.readFromFiles(dataFiles, false, entryConsumer);
        } finally {
            returnFiles.accept(dataFiles);
        }
    }

    public byte[] randomGet(final int key) throws IOException, StormDBException {
        int recordIndex;
        final RandomAccessFileWrapper f;
        byte[] value;
        rwLock.readLock().lock();
        final long address;
        try {
            recordIndex = index.get(key);
            if (recordIndex == NO_MAPPING_FOUND) { // No mapping value.
                return null; // NOSONAR - returning null is a part of the interface.
            }

            value = new byte[conf.getValueSize()];

            if (isCompactionInProgress() && compactionState.dataInNextWalFile.get(key)) {
                address = RecordUtil.indexToAddress(recordSize, recordIndex);
                if (address >= bytesInWalFile) {
                    System.arraycopy(buffer.array(),
                            (int) (address - bytesInWalFile + Config.KEY_SIZE),
                            value, 0, conf.getValueSize());
                    return value;
                }
                f = filePool.borrowObject(compactionState.nextWalFile);
            } else if (isCompactionInProgress() && compactionState.dataInNextFile.get(key)) {
                address = RecordUtil.indexToAddress(recordSize, recordIndex);
                f = filePool.borrowObject(compactionState.nextDataFile);
            } else if (dataInWalFile.get(key)) {
                address = RecordUtil.indexToAddress(recordSize, recordIndex);
                // If compaction is in progress, we can not read in-memory.
                if (!isCompactionInProgress() && address >= bytesInWalFile) {
                    System.arraycopy(buffer.array(),
                            (int) (address - bytesInWalFile + Config.KEY_SIZE),
                            value, 0, conf.getValueSize());
                    return value;
                }
                f = filePool.borrowObject(walFile);
            } else {
                address = RecordUtil.indexToAddress(recordSize, recordIndex);
                f = filePool.borrowObject(dataFile);
            }
        } finally {
            rwLock.readLock().unlock();
        }

        try {
            f.seek(address);
            if (f.readInt() != key) {
                throw new InconsistentDataException();
            }
            final int bytesRead = f.read(value);
            if (bytesRead != conf.getValueSize()) {
                throw new StormDBException("Possible data corruption detected! "
                        + "Re-open the database for automatic recovery!");
            }
            return value;
        } finally {
            filePool.returnObject(f.getFile(), f);
        }
    }

    public void close() throws IOException, InterruptedException {
        flush();
        shutDown = true;
        if (useExecutorService) {
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
        if (executorService != null) {
            esShutDown = true;
            synchronized (commonCompactionSync) {
                commonCompactionSync.notifyAll();
            }
            executorService.shutdown();
            if (!executorService.awaitTermination(5L * 60, TimeUnit.SECONDS)) {
                LOG.error("Unable to shutdown StormDB executor service in 5 minutes.");
            }
            executorService = null;
        }
    }

    public Config getConf() {
        return conf;
    }

    public boolean isUsingExecutorService() {
        return useExecutorService;
    }

    public int size() {
        return index.size();
    }
}
