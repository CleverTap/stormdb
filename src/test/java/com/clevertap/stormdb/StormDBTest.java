package com.clevertap.stormdb;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StormDBTest {

    @Test
    void simpleTest() throws IOException {
        final Path path = Files.createTempDirectory("stormdb");

        final int valueSize = 28;
        final StormDB db = new StormDB(valueSize, path.toString());

        final int records = 100;
        for (int i = 0; i < records; i++) {
            final ByteBuffer value = ByteBuffer.allocate(valueSize);
            value.putInt((int) (Math.random() * 100000000)); // Insert a random value.
            db.put(i, value.array());

            value.clear();
            value.putInt(i); // Insert a predictable value.
            db.put(i, value.array());
        }

        // Verify.
        for (int i = 0; i < records; i++) {
            final byte[] bytes = db.randomGet(i);
            final ByteBuffer value = ByteBuffer.wrap(bytes);
            assertEquals(i, value.getInt());
        }

        // Iterate sequentially.
        db.iterate(new EntryConsumer() {
            @Override
            public void accept(int key, byte[] data, int offset) {
                final ByteBuffer value = ByteBuffer.wrap(data, offset, valueSize);
                assertEquals(key, value.getInt());
            }
        });
    }

    @Test
    void compactionTest() throws IOException {
        final Path path = Files.createTempDirectory("stormdb");

        final int valueSize = 8;
        final StormDB db = new StormDB(valueSize, path.toString());

        final HashMap<Integer, Long> kvCache = new HashMap<>();

        final int totalRecords = 100;
        for (int i = 0; i < totalRecords; i++) {
            long val = (long) (Math.random() * Long.MAX_VALUE);
            final ByteBuffer value = ByteBuffer.allocate(valueSize);
            value.putLong(val); // Insert a random value.
            db.put(i, value.array());
            kvCache.put(i, val);
        }

        // Make sure all is well
        verifyDb(db, totalRecords, kvCache);

        // Now compact
        db.compact();

        // Make sure all is well
        verifyDb(db, totalRecords, kvCache);

        int count = 50;
        while(count-- > 0) {
            final ByteBuffer value = ByteBuffer.allocate(valueSize);
            long val = (long) (Math.random() * Long.MAX_VALUE);
            value.putLong(val); // Insert a random value.
            final int randomKey = (int)(Math.random()*totalRecords);
            db.put(randomKey, value.array());
            kvCache.put(randomKey, val);
        }

        // Make sure all is well
        verifyDb(db, totalRecords, kvCache);

        db.compact();

        // Make sure all is well
        verifyDb(db, totalRecords, kvCache);
    }

    private void verifyDb(StormDB db, int records, HashMap<Integer, Long> kvCache) throws IOException {
        // Verify.
        for (int i = 0; i < records; i++) {
            final byte[] bytes = db.randomGet(i);
            final ByteBuffer value = ByteBuffer.wrap(bytes);
            assertEquals(kvCache.get(i), value.getLong());
        }
    }
}