package com.clevertap.stormdb;

import com.clevertap.stormdb.exceptions.IncorrectConfigException;
import com.clevertap.stormdb.index.IndexMap;
import java.io.IOException;
import java.nio.file.Path;

public class StormDBBuilder {

    private final Config conf = new Config();

    /**
     * Disable compaction. This is a bad idea generally, as the WAL file can grow infinitely.
     * <p>
     * Call {@link StormDB#compact()} to compact the DB.
     * <p>
     * Default: true
     */
    public StormDBBuilder withAutoCompactDisabled() {
        conf.autoCompact = false;
        return this;
    }

    /**
     * Set the size of the value for each key. Once a value size has been set, it cannot be changed.
     * Re-opening the database with a different value size will throw a runtime exception.
     *
     * @param valueSize The size of each value, in bytes
     */
    public StormDBBuilder withValueSize(int valueSize) {
        conf.valueSize = valueSize;
        return this;
    }

    /**
     * The base directory for the database.
     *
     * @param dbDir An absolute path on the filesystem
     */
    public StormDBBuilder withDbDir(String dbDir) {
        conf.dbDir = dbDir;
        return this;
    }

    /**
     * Set the interval at which the database will be checked for auto compaction.
     * <p>
     * Note: If an executor service has been initialised via {@link StormDB#initExecutorService(int)},
     * then this value is ignored.
     * <p>
     * Default: {@link Config#DEFAULT_COMPACTION_WAIT_TIMEOUT_MS}
     *
     * @param compactionWaitTimeoutMs The timeout in milliseconds
     */
    public StormDBBuilder withCompactionWaitTimeoutMs(long compactionWaitTimeoutMs) {
        conf.compactionWaitTimeoutMs = compactionWaitTimeoutMs;
        return this;
    }

    /**
     * Set the threshold for the minimum number of buffers in the WAL file before compaction can
     * take place.
     * <p>
     * Default: {@link Config#DEFAULT_MIN_BUFFERS_TO_COMPACT}
     */
    public StormDBBuilder withMinBuffersToCompact(int minBuffersToCompact) {
        conf.minBuffersToCompact = minBuffersToCompact;
        return this;
    }

    /**
     * Set the ratio at which compaction will be triggered automatically if the WAL file grows
     * larger than the data file.
     * <p>
     * Default: {@link Config#DEFAULT_DATA_TO_WAL_FILE_RATIO}
     *
     * @param dataToWalFileRatio The ratio (valid values range from 1 to 100)
     */
    public StormDBBuilder withDataToWalFileRatio(int dataToWalFileRatio) {
        conf.dataToWalFileRatio = dataToWalFileRatio;
        return this;
    }

    /**
     * Set the interval at which the buffer will be flushed automatically.
     * <p>
     * Default: {@link Config#DEFAULT_BUFFER_FLUSH_TIMEOUT_MS}
     *
     * @param bufferFlushTimeoutMs The timeout in milliseconds
     */
    public StormDBBuilder withBufferFlushTimeoutMs(long bufferFlushTimeoutMs) {
        conf.bufferFlushTimeoutMs = bufferFlushTimeoutMs;
        return this;
    }

    /**
     * Set the maximum size of the WAL write buffer.
     * <p>
     * Default: {@link Config#DEFAULT_MAX_BUFFER_SIZE}
     *
     * @param maxBufferSize The size of the buffer, in bytes
     */
    public StormDBBuilder withMaxBufferSize(int maxBufferSize) {
        conf.maxBufferSize = maxBufferSize;
        return this;
    }

    /**
     * Option to specify custom index map based on application requirement.
     *
     * @param indexMap Custom map instance.
     */
    public StormDBBuilder withCustomIndexMap(IndexMap indexMap) {
        conf.indexMap = indexMap;
        return this;
    }

    /**
     * Set the database path.
     */
    public StormDBBuilder withDbDir(Path path) {
        return withDbDir(path.toString());
    }

    /**
     * Set the maximum number of open files permitted. If you have many threads, setting this to a
     * higher value (at least to the total number of threads) will improve the throughput of {@link
     * StormDB#randomGet(int)}. Internally, a file pool is used.
     * <p>
     * Note: Since at any given point in time, a data file and a WAL file exist, there will be at
     * most 2x open files in the system. During compaction, there will be 4x open files, however
     * that's temporary.
     *
     * @param openFDCount The number of open files allowed, per file type
     */
    public StormDBBuilder withMaxOpenFDCount(int openFDCount) {
        conf.openFDCount = openFDCount;
        return this;
    }

    public StormDB build() throws IOException {
        if (conf.dbDir == null || conf.dbDir.isEmpty()) {
            throw new IncorrectConfigException("StormDB directory path cannot be empty or null.");
        }
        if (conf.valueSize == 0) {
            throw new IncorrectConfigException("ValueSize cannot be 0.");
        }
        if (conf.compactionWaitTimeoutMs < Config.MIN_COMPACTION_WAIT_TIMEOUT_MS) {
            throw new IncorrectConfigException("Compaction timeout cannot be less than " +
                    Config.MIN_COMPACTION_WAIT_TIMEOUT_MS);
        }
        if (conf.bufferFlushTimeoutMs < 0) {
            throw new IncorrectConfigException("Buffer flush timeout cannot be less than 0");
        }

        if (conf.minBuffersToCompact < Config.FLOOR_MIN_BUFFERS_TO_COMPACT) {
            throw new IncorrectConfigException("Min buffers to compact cannot be less than " +
                    Config.FLOOR_MIN_BUFFERS_TO_COMPACT);
        }
        if (conf.dataToWalFileRatio < Config.MIN_DATA_TO_WAL_FILE_RATIO) {
            throw new IncorrectConfigException("Data to wal size ratio cannot be less than " +
                    Config.MIN_DATA_TO_WAL_FILE_RATIO);
        }
        if (conf.dataToWalFileRatio > Config.MAX_DATA_TO_WAL_FILE_RATIO) {
            throw new IncorrectConfigException("Data to wal size ratio cannot be greater than " +
                    Config.MAX_DATA_TO_WAL_FILE_RATIO);
        }
        if (conf.openFDCount > Config.MAX_OPEN_FD_COUNT) {
            throw new IncorrectConfigException("Open FD count cannot be greater than " +
                    Config.MAX_OPEN_FD_COUNT);
        }
        if (conf.openFDCount < Config.MIN_OPEN_FD_COUNT) {
            throw new IncorrectConfigException("Open FD count cannot be less than " +
                    Config.MIN_OPEN_FD_COUNT);
        }

        return new StormDB(conf);
    }
}
