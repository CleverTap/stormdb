package com.clevertap.stormdb;

import com.clevertap.stormdb.exceptions.StormDBException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

//@BenchmarkMode({Mode.All})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
//@State(Scope.Benchmark)
//@Fork(value = 2, jvmArgs = {"-Xms2G", "-Xmx2G"})
public class StormDBBenchmark {

    // TODO: 15/07/20 Sequential read - iterate
    // TODO: 15/07/20 Random put
    // TODO: 15/07/20 Random get
    // TODO: 15/07/20 compaction
    // TODO: 15/07/20 Combination of above 4

    @State(Scope.Benchmark)
    public static class BenchmarkStateWrite {

        //        @Param({"1000000", "10000000", "100000000"})
        @Param({"8"})
        public int valueSize;

        @Param({"1000"})
        public int maxUserSize;

        public StormDB db;

        public byte[][] testValues;
        public int i;

        @Setup(Level.Iteration)
        public void setUp() throws IOException {
            final Path path = Files.createTempDirectory("stormdb");
            db = new StormDBBuilder()
                    .withDbDir(path.toString())
                    .withValueSize(valueSize)
                    .build();
            testValues = new byte[maxUserSize][];
            // TODO: 15/07/20 Limit random bytes to a ceiling.
            Random random = new Random();
            for (int i = 0; i < maxUserSize; i++) {
                testValues[i] = new byte[valueSize];
                random.nextBytes(testValues[i]);
            }
            path.toFile().deleteOnExit();
        }
    }

    @State(Scope.Benchmark)
    public static class BenchmarkStateRead {

        //        @Param({"1000000", "10000000", "100000000"})
        @Param({"8"})
        public int valueSize;

        @Param({"1000"})
        public int maxUserSize;

        public StormDB db;

        public byte[][] testValues;

        public int[] randomKeys;
        public int i;

        @Setup(Level.Trial)
        public void setUp() throws IOException {
            final Path path = Files.createTempDirectory("stormdb");
            db = new StormDBBuilder()
                    .withDbDir(path.toString())
                    .withValueSize(valueSize)
                    .withAutoCompactDisabled()
                    .build();
            testValues = new byte[maxUserSize][];
            randomKeys = new int[maxUserSize];
            // TODO: 15/07/20 Limit random bytes to a ceiling.
            Random random = new Random();
            for (int i = 0; i < maxUserSize; i++) {
                testValues[i] = new byte[valueSize];
                random.nextBytes(testValues[i]);
                randomKeys[i] = (int) (Math.random() * maxUserSize);
            }

            for (int i = 0; i < maxUserSize; i++) {
                db.put(i, testValues[i]);
            }

            db.compact();
        }

        @Setup(Level.Iteration)
        public void reset() {
            i = 0;
        }
    }

    @Fork(value = 1, warmups = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void dbWrite(BenchmarkStateWrite state) throws IOException {
        state.db.put(state.i, state.testValues[Math.abs(state.i++) % state.maxUserSize]);
    }

    @Fork(value = 1, warmups = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void dbRead(Blackhole blackhole, BenchmarkStateRead state)
            throws IOException, StormDBException {
        blackhole.consume(
                state.db.randomGet(state.randomKeys[Math.abs(state.i++) % state.maxUserSize]));
    }

    @Fork(value = 1, warmups = 1)
    @Warmup(iterations = 1)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void dbIterate(Blackhole blackhole, BenchmarkStateRead state)
            throws IOException, StormDBException {
        StormDB db = state.db;
        db.iterate((key, data, offset) -> {
            blackhole.consume(key);
        });
    }

//    @Fork(value = 1, warmups = 1)
//    @Warmup(iterations = 1)
//    @Benchmark
//    @BenchmarkMode(Mode.AverageTime)
//    public void longAdder(Blackhole blackhole, BenchmarkState state) {
//        LongAdder adder = new LongAdder();
//        state.testList.parallelStream().forEach(adder::add);
//        blackhole.consume(adder.sum());
//    }

//    @Fork(value = 1, warmups = 1)
//    @Warmup(iterations = 1)
//    @Benchmark
//    @BenchmarkMode(Mode.AverageTime)
//    public void atomicLong(Blackhole blackhole, BenchmarkState state) {
//        AtomicLong atomicLong = new AtomicLong();
//        state.testList.parallelStream().forEach(atomicLong::addAndGet);
//        blackhole.consume(atomicLong.get());
//    }
//
//    @Fork(value = 1, warmups = 1)
//    @Warmup(iterations = 1)
//    @Benchmark
//    @BenchmarkMode(Mode.AverageTime)
//    public void volatileLong(Blackhole blackHole, BenchmarkState state) {
//        VolatileLong volatileLong = new VolatileLong();
//        state.testList.parallelStream().forEach(volatileLong::add);
//        blackHole.consume(volatileLong.getValue());
//    }
//
//    @Fork(value = 1, warmups = 1)
//    @Warmup(iterations = 1)
//    @Benchmark
//    @BenchmarkMode(Mode.AverageTime)
//    public void longStreamSum(Blackhole blackHole, BenchmarkState state) {
//        long sum = state.testList.parallelStream().mapToLong(s -> s).sum();
//        blackHole.consume(sum);
//    }

    public static void main(String[] args) throws RunnerException {

        Options opt = new OptionsBuilder()
                .include(StormDBBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }


}
