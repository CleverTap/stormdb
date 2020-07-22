package com.clevertap.stormdb;

import com.clevertap.stormdb.exceptions.IncorrectConfigException;
import java.io.IOException;
import java.nio.file.Path;

public class StormDBBuilder {

    private final Config conf = new Config();

    public StormDBBuilder withAutoCompactDisabled() {
        conf.autoCompact = false;
        return this;
    }

    public StormDBBuilder withValueSize(int valueSize) {
        conf.valueSize = valueSize;
        return this;
    }

    public StormDBBuilder withDbDir(String dbDir) {
        conf.dbDir = dbDir;
        return this;
    }

    public StormDBBuilder withCompactionWaitTimeoutMs(long compactionWaitTimeoutMs) {
        conf.compactionWaitTimeoutMs = compactionWaitTimeoutMs;
        return this;
    }

    public StormDBBuilder withMinBuffersToCompact(int minBuffersToCompact) {
        conf.minBuffersToCompact = minBuffersToCompact;
        return this;
    }

    public StormDBBuilder withDataToWalFileRatio(int dataToWalFileRatio) {
        conf.dataToWalFileRatio = dataToWalFileRatio;
        return this;
    }

    public StormDBBuilder withBufferFlushTimeoutMs(long bufferFlushTimeoutMs) {
        conf.bufferFlushTimeoutMs = bufferFlushTimeoutMs;
        return this;
    }

    public StormDBBuilder withMaxBufferSize(int maxBufferSize) {
        conf.maxBufferSize = maxBufferSize;
        return this;
    }

    public StormDBBuilder withDbDir(Path path) {
        return withDbDir(path.toString());
    }

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
