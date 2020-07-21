package com.clevertap.stormdb.internal;

import com.clevertap.stormdb.RandomAccessFileWrapper;
import com.clevertap.stormdb.exceptions.StormDBRuntimeException;
import java.io.File;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

/**
 * Created by Jude Pereira, at 14:15 on 21/07/2020.
 */
public class RandomAccessFilePool extends GenericKeyedObjectPool<File, RandomAccessFileWrapper> {

    public RandomAccessFilePool(final int openFds) {
        super(new RandomAccessFileFactory(), getConfig(openFds));
    }

    private static GenericKeyedObjectPoolConfig<RandomAccessFileWrapper> getConfig(int openFds) {
        final GenericKeyedObjectPoolConfig<RandomAccessFileWrapper> config =
                new GenericKeyedObjectPoolConfig<>();
        config.setMaxTotalPerKey(openFds);
        config.setBlockWhenExhausted(true);
        config.setTestOnBorrow(true);
        config.setTestOnCreate(true);
        return config;
    }

    @Override
    public RandomAccessFileWrapper borrowObject(File key) {
        try {
            return super.borrowObject(key);
        } catch (Exception e) {
            // This will never happen, unless opening a new file handle itself fails.
            throw new StormDBRuntimeException(e);
        }
    }
}
