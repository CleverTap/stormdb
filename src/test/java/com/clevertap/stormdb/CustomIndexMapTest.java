package com.clevertap.stormdb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.clevertap.stormdb.exceptions.StormDBException;
import com.clevertap.stormdb.index.IndexMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import org.junit.jupiter.api.Test;

public class CustomIndexMapTest {

    @Test
    public void testCustomMap() throws IOException, StormDBException {
        final Path path = Files.createTempDirectory("stormdb");
        final int valueSize = 8;
        final HashMap<Integer, Integer> kvCache = new HashMap<>();
        final int[] activityCount = {0,0};
        final StormDB db = new StormDBBuilder()
                .withDbDir(path.toString())
                .withValueSize(valueSize)
                .withAutoCompactDisabled()
                .withCustomIndexMap(new IndexMap() {
                    @Override
                    public void put(int key, int indexValue) {
                        activityCount[0]++;
                        kvCache.put(key, indexValue);
                    }

                    @Override
                    public int get(int key) {
                        activityCount[1]++;
                        return kvCache.get(key);
                    }

                    @Override
                    public int size() {
                        return kvCache.size();
                    }
                })
                .build();

        assertEquals(0, db.size());

        final ByteBuffer value = ByteBuffer.allocate(valueSize);
        value.putLong(100L);
        db.put(0, value.array());
        assertEquals(1, activityCount[0]);

        final byte[] bytes = db.randomGet(0);
        final ByteBuffer valueGet = ByteBuffer.wrap(bytes);
        assertEquals(100, valueGet.getLong());
        assertEquals(1, activityCount[0]);

        assertEquals(1, db.size());

    }

}
