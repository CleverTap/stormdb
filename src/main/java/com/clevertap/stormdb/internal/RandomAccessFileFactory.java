package com.clevertap.stormdb.internal;

import com.clevertap.stormdb.RandomAccessFileWrapper;
import java.io.File;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * Created by Jude Pereira, at 14:18 on 21/07/2020.
 */
public class RandomAccessFileFactory implements
        KeyedPooledObjectFactory<File, RandomAccessFileWrapper> {

    @Override
    public PooledObject<RandomAccessFileWrapper> makeObject(File file) throws Exception {
        return new DefaultPooledObject<>(new RandomAccessFileWrapper(file, "r"));
    }

    @Override
    public void destroyObject(File file, PooledObject<RandomAccessFileWrapper> pooledObject)
            throws Exception {
        pooledObject.getObject().close();
    }

    @Override
    public boolean validateObject(File file, PooledObject<RandomAccessFileWrapper> pooledObject) {
        return pooledObject.getObject().isSameFile(file);
    }

    @Override
    public void activateObject(File file, PooledObject<RandomAccessFileWrapper> pooledObject) {
        // No action required.
    }

    @Override
    public void passivateObject(File file, PooledObject<RandomAccessFileWrapper> pooledObject) {
        // No action required.
    }
}
