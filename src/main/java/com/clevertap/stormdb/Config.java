package com.clevertap.stormdb;

import com.clevertap.stormdb.maps.IndexMap;
import java.nio.ByteBuffer;

public class Config {

    // Block parameters
    public static final int RECORDS_PER_BLOCK = 128; // Not configurable for now.
    public static final int CRC_SIZE = 4; // CRC32. Not configurable for now.

    // Key size
    static final int KEY_SIZE = 4; // Not Configurable for now.

    // Compaction parameter defaults
    private static final long DEFAULT_COMPACTION_WAIT_TIMEOUT_MS =
            (long) 1000 * 60; // 1 minute for now.
    static final long MIN_COMPACTION_WAIT_TIMEOUT_MS = (long) 1000 * 30; // 1/2 minute for now.
    private static final int DEFAULT_MIN_BUFFERS_TO_COMPACT = 8;
    static final int FLOOR_MIN_BUFFERS_TO_COMPACT = 4; // Min 4 buffers to compact for now
    private static final int DEFAULT_DATA_TO_WAL_FILE_RATIO = 10;
    static final int MIN_DATA_TO_WAL_FILE_RATIO = 2;
    static final int MAX_DATA_TO_WAL_FILE_RATIO = 100;

    // Buffer parameter defaults
    private static final long DEFAULT_BUFFER_FLUSH_TIMEOUT_MS =
            (long) 1000 * 60; // 1 minute for now.
    private static final int DEFAULT_MAX_BUFFER_SIZE = 4 * 1024 * 1024;
    /**
     * Theoretically, this can go somewhere up to 15 MB, however, the corresponding buffer size will
     * be 1.5 GB. We leave this to 512 KB since it's a fairly high value.
     * <p>
     * Note: The hard limit is due to the fact that {@link ByteBuffer} accepts an int as its size.
     */
    static final int MAX_VALUE_SIZE = 512 * 1024; // Not configurable for now.

    // File open fd parameter defaults and range
    private static final int DEFAULT_OPEN_FD_COUNT = 10;
    static final int MIN_OPEN_FD_COUNT = 1;
    static final int MAX_OPEN_FD_COUNT = 100;

    static final int MAX_KEYS_IN_SET_FOR_DELETION = 20; // every 20 keys write to deletion files

    // Must have parameters
    boolean autoCompact = true;
    int valueSize;
    String dbDir;

    // Other parameters
    long compactionWaitTimeoutMs = DEFAULT_COMPACTION_WAIT_TIMEOUT_MS;
    int minBuffersToCompact = DEFAULT_MIN_BUFFERS_TO_COMPACT;
    int dataToWalFileRatio = DEFAULT_DATA_TO_WAL_FILE_RATIO;
    long bufferFlushTimeoutMs = DEFAULT_BUFFER_FLUSH_TIMEOUT_MS;
    int maxBufferSize = DEFAULT_MAX_BUFFER_SIZE;
    int openFDCount = DEFAULT_OPEN_FD_COUNT;
    IndexMap indexMap;

    public boolean autoCompactEnabled() {
        return autoCompact;
    }

    public int getValueSize() {
        return valueSize;
    }

    public String getDbDir() {
        return dbDir;
    }

    public static long getDefaultCompactionWaitTimeoutMs() {
        return DEFAULT_COMPACTION_WAIT_TIMEOUT_MS;
    }

    /**
     * @return Compaction wait timeout in msec. With executor service {@link
     * Config#DEFAULT_COMPACTION_WAIT_TIMEOUT_MS} will be used.
     */
    public long getCompactionWaitTimeoutMs() {
        return compactionWaitTimeoutMs;
    }

    public int getMinBuffersToCompact() {
        return minBuffersToCompact;
    }

    public int getDataToWalFileRatio() {
        return dataToWalFileRatio;
    }

    public long getBufferFlushTimeoutMs() {
        return bufferFlushTimeoutMs;
    }

    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    public int getOpenFDCount() {
        return openFDCount;
    }

    public IndexMap getIndexMap() { return indexMap; }

}
