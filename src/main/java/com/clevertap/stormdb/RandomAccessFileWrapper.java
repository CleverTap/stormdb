package com.clevertap.stormdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

/**
 * We need to create this wrapper primarily to check if the RandomAccessFile handle has been invalidated
 * during compaction. The file object is then directly ref compared to see whether it is valid.
 */
public class RandomAccessFileWrapper extends RandomAccessFile {
    private final File fileObject;
    public RandomAccessFileWrapper(File f, String mode) throws FileNotFoundException {
        super(f, mode);
        fileObject = f;
    }

    public boolean isSameFile(File f) {
        return fileObject == f;
    }
}
