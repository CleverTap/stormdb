package com.clevertap.stormdb;

import com.clevertap.stormdb.exceptions.IncorrectConfigException;
import java.io.IOException;
import java.nio.file.Path;

public class StormDBBuilder {

    private final StormDBConfig dbConfig = new StormDBConfig();

    public StormDBBuilder withAutoCompactDisabled() {
        dbConfig.autoCompact = false;
        return this;
    }

    public StormDBBuilder withValueSize(int valueSize) {
        dbConfig.valueSize = valueSize;
        return this;
    }

    public StormDBBuilder withDbDir(String dbDir) {
        dbConfig.dbDir = dbDir;
        return this;
    }

    public StormDBBuilder withCustomCompactionWaitTimeoutMs(long compactionWaitTimeoutMs) {
        dbConfig.compactionWaitTimeoutMs = compactionWaitTimeoutMs;
        return this;
    }

    public StormDBBuilder withCustomMinBuffersToCompact(int minBuffersToCompact) {
        dbConfig.minBuffersToCompact = minBuffersToCompact;
        return this;
    }

    public StormDBBuilder withCustomDataToWalFileRatio(int dataToWalFileRatio) {
        dbConfig.dataToWalFileRatio = dataToWalFileRatio;
        return this;
    }

    public StormDBBuilder withCustomBufferFlushTimeoutMs(long bufferFlushTimeoutMs) {
        dbConfig.bufferFlushTimeoutMs = bufferFlushTimeoutMs;
        return this;
    }

    public StormDBBuilder withCustomMaxBufferSize(int maxBufferSize) {
        dbConfig.maxBufferSize = maxBufferSize;
        return this;
    }

    public StormDBBuilder withDbDir(Path path) {
        return withDbDir(path.toString());
    }

    public StormDBBuilder withCustomOpenFDCount(int openFDCount) {
        dbConfig.openFDCount = openFDCount;
        return this;
    }

    public StormDB build() throws IOException {
        if (dbConfig.dbDir == null || dbConfig.dbDir.isEmpty()) {
            throw new IncorrectConfigException("StormDB directory path cannot be empty or null.");
        }
        if (dbConfig.valueSize == 0) {
            throw new IncorrectConfigException("ValueSize cannot be 0.");
        }
        if (dbConfig.compactionWaitTimeoutMs < StormDBConfig.MIN_COMPACTION_WAIT_TIMEOUT_MS) {
            throw new IncorrectConfigException("Compaction timeout cannot be less than " +
                    StormDBConfig.MIN_COMPACTION_WAIT_TIMEOUT_MS);
        }
        if (dbConfig.minBuffersToCompact < StormDBConfig.FLOOR_MIN_BUFFERS_TO_COMPACT) {
            throw new IncorrectConfigException("Min buffers to compact cannot be less than " +
                    StormDBConfig.FLOOR_MIN_BUFFERS_TO_COMPACT);
        }
        if (dbConfig.dataToWalFileRatio < StormDBConfig.MIN_DATA_TO_WAL_FILE_RATIO) {
            throw new IncorrectConfigException("Data to wal size ratio cannot be less than " +
                    StormDBConfig.MIN_DATA_TO_WAL_FILE_RATIO);
        }
        if (dbConfig.dataToWalFileRatio > StormDBConfig.MAX_DATA_TO_WAL_FILE_RATIO) {
            throw new IncorrectConfigException("Data to wal size ratio cannot be greater than " +
                    StormDBConfig.MAX_DATA_TO_WAL_FILE_RATIO);
        }
        if (dbConfig.openFDCount > StormDBConfig.MAX_OPEN_FD_COUNT) {
            throw new IncorrectConfigException("Open FD count cannot be greater than " +
                    StormDBConfig.MAX_OPEN_FD_COUNT);
        }
        if (dbConfig.openFDCount < StormDBConfig.MIN_OPEN_FD_COUNT) {
            throw new IncorrectConfigException("Open FD count cannot be less than " +
                    StormDBConfig.MIN_OPEN_FD_COUNT);
        }

        return new StormDB(dbConfig);
    }
}
