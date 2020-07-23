package com.clevertap.stormdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.clevertap.stormdb.exceptions.IncorrectConfigException;
import com.clevertap.stormdb.exceptions.ReservedKeyException;
import com.clevertap.stormdb.exceptions.StormDBException;
import com.clevertap.stormdb.exceptions.StormDBRuntimeException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class StormDBTest {

    @Test
    void simpleTest() throws IOException, StormDBException, InterruptedException {
        final Path path = Files.createTempDirectory("stormdb");

        final int valueSize = 28;
        final StormDB db = new StormDBBuilder()
                .withDbDir(path.toString())
                .withValueSize(valueSize)
                .withAutoCompactDisabled()
                .build();

        assertEquals(0, db.size());

        final int records = 100;
        for (int i = 0; i < records; i++) {
            assertNull(db.randomGet(i));
            final ByteBuffer value = ByteBuffer.allocate(valueSize);
            value.putInt((int) (Math.random() * 100000000)); // Insert a random value.
            db.put(i, value.array());

            value.clear();
            value.putInt(i); // Insert a predictable value.
            db.put(i, value.array());
        }

        assertEquals(records, db.size());

        // Verify.
        for (int i = 0; i < records; i++) {
            final byte[] bytes = db.randomGet(i);
            final ByteBuffer value = ByteBuffer.wrap(bytes);
            assertEquals(i, value.getInt());
        }

        // Iterate sequentially.
        db.iterate((key, data, offset) -> {
            final ByteBuffer value = ByteBuffer.wrap(data, offset, valueSize);
            assertEquals(key, value.getInt());
        });

        db.close();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4,
            Config.RECORDS_PER_BLOCK - 1,
            Config.RECORDS_PER_BLOCK,
            Config.RECORDS_PER_BLOCK + 1,
            100, 1000, 10_000, 100_000, 200_000, 349_440})
    void compactionTest(final int totalRecords)
            throws IOException, StormDBException, InterruptedException {
        final Path path = Files.createTempDirectory("stormdb");

        final int valueSize = 8;
        final StormDB db = new StormDBBuilder()
                .withDbDir(path.toString())
                .withValueSize(valueSize)
                .withAutoCompactDisabled()
                .build();

        final HashMap<Integer, Long> kvCache = new HashMap<>();

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

        int count = totalRecords / 2;
        while (count-- > 0) {
            final ByteBuffer value = ByteBuffer.allocate(valueSize);
            long val = (long) (Math.random() * Long.MAX_VALUE);
            value.putLong(val); // Insert a random value.
            final int randomKey = (int) (Math.random() * totalRecords);
            db.put(randomKey, value.array());
            kvCache.put(randomKey, val);
        }

        // Make sure all is well
        verifyDb(db, totalRecords, kvCache);

        db.compact();

        // Make sure all is well
        verifyDb(db, totalRecords, kvCache);

        db.close();
    }

    private void verifyDb(StormDB db, int records, HashMap<Integer, Long> kvCache)
            throws IOException, StormDBException {
        // Verify.
        for (int i = 0; i < records; i++) {
            final byte[] bytes = db.randomGet(i);
            final ByteBuffer value = ByteBuffer.wrap(bytes);
            assertEquals(kvCache.get(i), value.getLong());
        }
    }

    @Test
    void verifyPersistenceOfValueSize() throws IOException, InterruptedException {
        final String dbDir = Files.createTempDirectory("storm").toString();
        final StormDB db = new StormDBBuilder()
                .withDbDir(dbDir)
                .withValueSize(8)
                .build();

        db.close();

        final StormDBBuilder builder = new StormDBBuilder()
                .withDbDir(dbDir)
                .withValueSize(16);
        assertThrows(IncorrectConfigException.class, builder::build);
    }

    @Test
    void testAutoCompaction() throws IOException, InterruptedException, StormDBException {
        // Create custom config and avoid builder
        final Config config = new Config();
        config.compactionWaitTimeoutMs = 100;
        config.valueSize = 8;
        config.dbDir = Files.createTempDirectory("storm").toString();
        config.minBuffersToCompact = 1;

        StormDB stormDB = new StormDB(config);

        final HashMap<Integer, Long> kvCache = new HashMap<>();
        int totalRecords = 1000_000;
        for (int i = 0; i < totalRecords; i++) {
            long val = (long) (Math.random() * Long.MAX_VALUE);
            final ByteBuffer value = ByteBuffer.allocate(config.getValueSize());
            value.putLong(val); // Insert a random value.
            stormDB.put(i, value.array());
            kvCache.put(i, val);
        }

        final long sleepTimeMs = 10;
        long numberIterations = config.compactionWaitTimeoutMs * 5 / sleepTimeMs + 1;
        while (numberIterations > 0) {
            Thread.sleep(sleepTimeMs);
            if (isCompactionComplete(config)) {
                break;
            }
            numberIterations--;
        }

        assertTrue(numberIterations > 0);

        // Make sure all is well
        verifyDb(stormDB, totalRecords, kvCache);
    }

    private boolean isCompactionComplete(Config conf) throws InterruptedException {
        File dataFile = new File(conf.getDbDir() + File.separator + "data");
        File walFile = new File(conf.getDbDir() + File.separator + "wal");
        File nextDataFile = new File(conf.getDbDir() + File.separator + "data.next");
        File nextWalFile = new File(conf.getDbDir() + File.separator + "wal.next");
        if (nextDataFile.exists()) {
            return false;
        }
        if (nextWalFile.exists()) {
            return false;
        }
        if (walFile.length() != 0) {
            return false;
        }
        if (dataFile.length() == 0) {
            return false;
        }
        return true;
    }

    @Test
    void testExecutorService() throws IOException, InterruptedException {
        StormDB.initExecutorService(2);
        final ArrayList<StormDB> allDbList = new ArrayList<>();

        final int totalInstances = 10;
        for (int i = 0; i < totalInstances; i++) {
            final Config config = new Config();
            config.valueSize = 8;
            config.dbDir = Files.createTempDirectory("storm").toString();
            config.minBuffersToCompact = 1;

            StormDB db = new StormDB(config);
            assertTrue(db.isUsingExecutorService());
            allDbList.add(db);

            final ByteBuffer value = ByteBuffer.allocate(8);
            int totalRecords = 1000_000;
            for (int j = 0; j < totalRecords; j++) {
                db.put(j, value.array());
            }
        }
        System.out.println("Finished writing data.");

        final ArrayList<StormDB> listDb = (ArrayList<StormDB>) allDbList.clone();
        final long sleepTimeMs = 10;
        final long numberIterations = listDb.size() * Config.getDefaultCompactionWaitTimeoutMs()
                * 5 / sleepTimeMs + 1;
        for (int i = 0; i < numberIterations; i++) {
            Thread.sleep(sleepTimeMs);
            if (isCompactionComplete(listDb.get(0).getConf())) {
                listDb.remove(0);
            }
            if (listDb.isEmpty()) {
                break;
            }
        }

        assertTrue(listDb.isEmpty());

        for (StormDB stormDB : allDbList) {
            stormDB.close();
        }

        StormDB.shutDownExecutorService();
    }

    @Test
    void testBufferFlushExecutorService() throws IOException, InterruptedException {
        StormDB.initExecutorService(2);
        final ArrayList<StormDB> allDbList = new ArrayList<>();

        final int totalInstances = 10;
        for (int i = 0; i < totalInstances; i++) {
            final Config config = new Config();
            config.bufferFlushTimeoutMs = 50;
            config.maxBufferSize = 1024 * 1024;
            config.valueSize = 8;
            config.dbDir = Files.createTempDirectory("storm").toString();

            StormDB db = new StormDB(config);
            assertTrue(db.isUsingExecutorService());
            allDbList.add(db);

            final ByteBuffer value = ByteBuffer.allocate(8);
            int totalRecords = 100_000;
            for (int j = 0; j < totalRecords; j++) {
                db.put(j, value.array());
            }
        }

        long sleepTimeMs = 10;
        ArrayList<StormDB> listDb = (ArrayList<StormDB>) allDbList.clone();
        long numberIterations = Config.getDefaultCompactionWaitTimeoutMs() * 5 / sleepTimeMs + 1;
        for (int i = 0; i < numberIterations; i++) {
            final StormDB stormDB = listDb.get(0);
            Thread.sleep(sleepTimeMs);
            File walFile = new File(stormDB.getConf().getDbDir() + File.separator + "wal");
//            System.out.println(walFile.toString());
            if (walFile.exists() && walFile.length() > 0) {
                listDb.remove(0);
            }
            if (listDb.isEmpty()) {
                break;
            }
        }

        assertTrue(listDb.isEmpty());

        for (StormDB stormDB : allDbList) {
            stormDB.close();
        }

        StormDB.shutDownExecutorService();
    }

    @Test
    void testMultipleConfigurations() throws IOException {
        final Path path = Files.createTempDirectory("storm");
        StormDB db = new StormDBBuilder()
                .withValueSize(100)
                .withDbDir(path)
                .withAutoCompactDisabled()
                .withCompactionWaitTimeoutMs(45 * 1000)
                .withBufferFlushTimeoutMs(30 * 1000)
                .withDataToWalFileRatio(25)
                .withMaxBufferSize(8 * 1024 * 1024)
                .withMinBuffersToCompact(5)
                .withMaxOpenFDCount(40)
                .build();

        final Config dbConfig = db.getConf();
        assertEquals(100, dbConfig.getValueSize());
        assertEquals(path.toString(), dbConfig.getDbDir());
        assertFalse(dbConfig.autoCompactEnabled());
        assertEquals(30 * 1000, dbConfig.getBufferFlushTimeoutMs());
        assertEquals(45 * 1000, dbConfig.getCompactionWaitTimeoutMs());
        assertEquals(25, dbConfig.getDataToWalFileRatio());
        assertEquals(8 * 1024 * 1024, dbConfig.getMaxBufferSize());
        assertEquals(5, dbConfig.getMinBuffersToCompact());
        assertEquals(40, dbConfig.getOpenFDCount());
    }

    @ParameterizedTest
    @ValueSource(strings={"wal.next", "data.next"})
    void recoverWithIncompleteCompaction(final String compactionFileName)
            throws IOException, InterruptedException, StormDBException {
        final Path dbPath = Files.createTempDirectory("storm");
        StormDB db = buildDB(dbPath, 100);

        final byte[] value1 = new byte[100];
        final byte[] value2 = new byte[100];
        ThreadLocalRandom.current().nextBytes(value1);
        ThreadLocalRandom.current().nextBytes(value2);

        db.put(1, value1);
        db.close();

        // Simulate a incomplete compaction by creating wal.next/data.next.
        final Buffer buffer = new Buffer(db.getConf(), false);
        buffer.add(2, value2, 0);

        buffer.flush(new FileOutputStream(new File(dbPath.toFile(), compactionFileName)));
        buffer.clear();

        db = buildDB(dbPath, 100);

        // Since wal.next/data.next had a record which wasn't present in the original database,
        // try to retrieve it.
        assertArrayEquals(value1, db.randomGet(1));
        assertArrayEquals(value2, db.randomGet(2));
    }

    @Test
    void flushSimulateInfiniteCompaction() throws IOException {
        final StormDB db = buildDB(Files.createTempDirectory("storm"), 10);

        final CompactionState state = Mockito.mock(CompactionState.class);
        when(state.getStart()).thenReturn(System.currentTimeMillis() - 31 * 60 * 1000L);
        when(state.runningForTooLong()).thenReturn(true);

        db.put(1, new byte[10]);
        
        db.compactionState = state;

        assertNull(db.exceptionDuringBackgroundOps);
        db.flush();

        assertNotNull(db.exceptionDuringBackgroundOps);
    }

    @Test
    void verifyPutFailure() throws IOException {
        final StormDB db = buildDB(Files.createTempDirectory("storm"), 100);
        db.put(1, new byte[100]);

        db.exceptionDuringBackgroundOps = new StormDBRuntimeException();
        assertThrows(StormDBRuntimeException.class, () -> db.put(1, new byte[100]));

        db.exceptionDuringBackgroundOps = null;
        db.put(1, new byte[100]);
    }

    @Test
    void recoverWithPartialWrites() throws IOException, InterruptedException, StormDBException {
        final Path dbPath = Files.createTempDirectory("storm");
        StormDB db = buildDB(dbPath, 100);

        final byte[] value1 = new byte[100];
        final byte[] value2 = new byte[100];
        ThreadLocalRandom.current().nextBytes(value1);
        ThreadLocalRandom.current().nextBytes(value2);

        db.put(1, value1);
        db.compact();
        db.put(2, value2);

        db.close();

        final File walFile = new File(dbPath.toFile(), "wal");
        final byte[] originalWalContent = Files
                .readAllBytes(walFile.toPath());

        final File dataFile = new File(dbPath.toFile(), "data");
        final byte[] originalDataContent = Files
                .readAllBytes(dataFile.toPath());

        // Simulate partial writes to the WAL and data files.
        Files.write(walFile.toPath(), new byte[100], StandardOpenOption.APPEND);
        Files.write(dataFile.toPath(), new byte[100], StandardOpenOption.APPEND);

        db = buildDB(dbPath, 100);

        assertArrayEquals(value1, db.randomGet(1));
        assertArrayEquals(value2, db.randomGet(2));

        db.close();

        // Verify that the data was successfully recovered.
        assertArrayEquals(originalDataContent, Files.readAllBytes(dataFile.toPath()));
        assertArrayEquals(originalWalContent, Files.readAllBytes(walFile.toPath()));
    }

    private StormDB buildDB(Path dbPath, int valueSize) throws IOException {
        return new StormDBBuilder()
                .withValueSize(valueSize)
                .withDbDir(dbPath)
                .build();
    }

    @Test
    void testIncorrectConfiguration() throws IOException {
        StormDBBuilder builder = new StormDBBuilder();
        assertThrows(IncorrectConfigException.class, builder::build);

        final Path path = Files.createTempDirectory("storm");
        builder = new StormDBBuilder().withDbDir(path);
        assertThrows(IncorrectConfigException.class, builder::build);

        builder = new StormDBBuilder().withDbDir(path).withValueSize(10)
                .withBufferFlushTimeoutMs(-1);
        assertThrows(IncorrectConfigException.class, builder::build);

        builder = new StormDBBuilder().withDbDir(path).withValueSize(10)
                .withCompactionWaitTimeoutMs(100);
        assertThrows(IncorrectConfigException.class, builder::build);

        for (int invalidBuffers : new int[]{0, 2}) {
            builder = new StormDBBuilder().withDbDir(path).withValueSize(10)
                    .withMinBuffersToCompact(invalidBuffers);
            assertThrows(IncorrectConfigException.class, builder::build);
        }

        for (int invalidRatio : new int[]{0, 101}) {
            builder = new StormDBBuilder().withDbDir(path).withValueSize(10)
                    .withDataToWalFileRatio(invalidRatio);
            assertThrows(IncorrectConfigException.class, builder::build);
        }

        for (int invalidOpenFds : new int[]{0, 101}) {
            builder = new StormDBBuilder().withDbDir(path).withValueSize(10)
                    .withMaxOpenFDCount(invalidOpenFds);
            assertThrows(IncorrectConfigException.class, builder::build);
        }
    }

    @Test
    void put() throws IOException, StormDBException {
        final StormDB db = new StormDBBuilder()
                .withDbDir(Files.createTempDirectory("storm"))
                .withValueSize(8)
                .build();

        assertThrows(ReservedKeyException.class,
                () -> db.put(StormDB.RESERVED_KEY_MARKER, new byte[1]));

        final byte[] value = new byte[8];
        ThreadLocalRandom.current().nextBytes(value);

        db.put(1, value);
        assertArrayEquals(value, db.randomGet(1));

        final byte[] largeByteArr = new byte[value.length + 100];
        System.arraycopy(value, 0, largeByteArr, 50, value.length);
        db.put(2, largeByteArr, 50);
        assertArrayEquals(value, db.randomGet(2));

        final byte[] key = new byte[4];
        final ByteBuffer keyBuf = ByteBuffer.wrap(key);
        keyBuf.putInt(Integer.MAX_VALUE - 200);
        db.put(key, value);
        assertArrayEquals(value, db.randomGet(Integer.MAX_VALUE - 200));

        keyBuf.clear();
        keyBuf.putInt(Integer.MAX_VALUE - 100);
        db.put(key, largeByteArr, 50);
        assertArrayEquals(value, db.randomGet(Integer.MAX_VALUE - 100));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 100, 1000, 10_000, 100_000, 1_000_000, 3_000_000})
    void testBuildIndex(final int totalRecords)
            throws IOException, InterruptedException, StormDBException {
        final Path path = Files.createTempDirectory("stormdb");
        System.out.println(path.toString() + " for " + totalRecords);
        final int valueSize = 8;

        StormDB db = new StormDBBuilder()
                .withDbDir(path.toString())
                .withValueSize(valueSize)
                .build();
        final HashMap<Integer, Long> kvCache = new HashMap<>();
        for (int i = 0; i < totalRecords; i++) {
            long val = i * 2;
            final ByteBuffer value = ByteBuffer.allocate(valueSize);
            value.putLong(val); // Insert a random value.
            db.put(i, value.array());
            kvCache.put(i, val);
        }
        db.close();

        db = new StormDBBuilder()
                .withDbDir(path.toString())
                .withValueSize(valueSize)
                .withAutoCompactDisabled()
                .build();
        // Verify here.
        verifyDb(db, totalRecords, kvCache);
        db.close();
    }

    @Test
    void testMultiThreaded() throws IOException, InterruptedException, StormDBException {
        final Path path = Files.createTempDirectory("stormdb");

        final int valueSize = 8;
        final StormDB db = new StormDBBuilder()
                .withDbDir(path.toString())
                .withValueSize(valueSize)
                .withAutoCompactDisabled()
                .build();

        final int totalRecords = 1_000_000;
        final int maxSleepMs = 100;
        final int[] timeToRunInSeconds = {10};
        long[] kvCache = new long[totalRecords];

        final Boolean[] exceptionOrAssertion = {false, false};
        final Boolean[] shutdown = {false};
        final ExecutorService service = Executors.newFixedThreadPool(4);

        // Writer thread.
        service.submit(() -> {
            while (!shutdown[0]) {
                for (int i = 0; i < totalRecords; i++) {
                    long val = (i % 1000);
                    final ByteBuffer value = ByteBuffer.allocate(valueSize);
                    value.putLong(val); // Insert a random value.
                    synchronized (kvCache) {
                        kvCache[i] = val; // Update cache first.
                    }
                    try {
                        db.put(i, value.array());
                    } catch (Exception e) {
                        e.printStackTrace();
                        exceptionOrAssertion[0] = true;
                    }
                }
                sleepRandomMs("writer", maxSleepMs);
            }
            System.out.println("Finished writer thread.");
        });

        // Compaction thread
        service.submit(() -> {
            while (!shutdown[0]) {
                try {
                    db.compact();
                } catch (Exception e) {
                    e.printStackTrace();
                    exceptionOrAssertion[0] = true;
                }
                sleepRandomMs("compaction", maxSleepMs);
            }
            System.out.println("Finished compaction thread.");
        });

        service.submit(() -> {
            while (!shutdown[0]) {
                // Iterate sequentially.
                try {
                    final int[] prevKey = {Integer.MIN_VALUE};
                    db.iterate((key, data, offset) -> {
                        assertNotEquals(prevKey[0], key);
                        prevKey[0] = key;
                        final ByteBuffer value = ByteBuffer.wrap(data, offset, valueSize);
                        if (kvCache[key] < value.getLong()) {
                            exceptionOrAssertion[1] = true;
                        }
                    });
                } catch (Exception t) {
                    exceptionOrAssertion[0] = true;
                }
//                sleepRandomMs("iterate", maxSleepMs);
            }
            System.out.println("Finished iteration thread.");
        });

        // Tracker thread
        service.submit(() -> {
            while (timeToRunInSeconds[0]-- > 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            shutdown[0] = true;
            System.out.println("Finished tracker thread. Exiting.");
        });

        // Verifier / Reader
        while (!shutdown[0]) {
            for (int i = 0; i < totalRecords; i++) {
                final byte[] bytes;
                try {
                    bytes = db.randomGet(i);
                    if (bytes == null) {
                        continue;
                    }
                    final ByteBuffer value = ByteBuffer.wrap(bytes);
                    final long longValue = value.getLong();
                    synchronized (kvCache) {
                        if (kvCache[i] < longValue) {
                            exceptionOrAssertion[1] = true;
                        }
                    }
                } catch (Exception t) {
                    exceptionOrAssertion[0] = true;
                }
            }
            sleepRandomMs("verifier", maxSleepMs);
        }
        System.out.println("Completed verification in main thread.");

        System.out.println("service.awaitTermination for 5 seconds started.");
        service.shutdown();
        assertTrue(service.awaitTermination(5, TimeUnit.SECONDS));
        db.close();
        assertFalse(exceptionOrAssertion[0]);
        assertFalse(exceptionOrAssertion[1]);
    }

    private void sleepRandomMs(String caller, int maxMs) {
        try {
            long sleepTime = (long) (maxMs * Math.random());
            System.out.println("Sleeping " + caller + " for " + sleepTime + " ms.");
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testMidWayFileDelete() throws IOException {
        // This tests java bug highlighted below. Can remove later.
        // https://stackoverflow.com/questions/991489/file-delete-returns-false-even-though-file-exists-file-canread-file-canw
        final int totalLines = 1000000;
        final Path path = Files.createTempDirectory("testdelete");
        final String tempFileName = path.toString() + "/temp.txt";
        try {
            FileWriter myWriter = new FileWriter(tempFileName);
            for (int i = 0; i < totalLines; i++) {
                myWriter.write("This is a test line.\n");
            }
            myWriter.close();
            System.out.println("Successfully wrote to the file.");

            File file = new File(tempFileName);    //creates a new file instance
            FileReader fr = new FileReader(file);   //reads the file
            BufferedReader br = new BufferedReader(fr,
                    128);  //creates a buffering character input stream
            int c = 0;
            while ((br.readLine()) != null) {
                if (c++ == totalLines / 2) {
                    assertTrue(file.delete());
                }
            }
            fr.close();    //closes the stream and release the resources
            assertEquals(totalLines, c);
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
}