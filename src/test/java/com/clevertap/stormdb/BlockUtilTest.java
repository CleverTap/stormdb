package com.clevertap.stormdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Created by Jude Pereira, at 16:02 on 16/07/2020.
 */
class BlockUtilTest {

    @Test
    void verifyBlocksGood() throws IOException {
        final Buffer buffer = new Buffer(100, false);
        buffer.add(1, new byte[100], 0);

        final Path tempPath = Files.createTempFile("stormdb_", "_block_util");
        final File tempFile = tempPath.toFile();
        tempFile.deleteOnExit();

        try (final FileOutputStream out = new FileOutputStream(tempFile)) {
            buffer.flush(out);
        }

        final File actual = BlockUtil.verifyBlocks(tempFile, 100);
        assertEquals(tempFile, actual);
    }

    private static Stream<Arguments> provideRecoveryCases() {
        final Builder<Arguments> builder = Stream.builder();

        for (boolean addTrailingGarbage : new boolean[]{true, false}) {
            for (boolean incompleteLastBlock : new boolean[]{true, false}) {
                for (boolean addGarbageHeader : new boolean[]{true, false}) {
                    for (boolean randomizeGarbage : new boolean[]{true, false}) {
                        for (boolean corruptEveryAlternateBlock : new boolean[]{true, false}) {
                            for (int blocks : new int[]{0, 1, 2, 10, 64, 128}) {
                                for (int valueSize : new int[]{1, 8, 16, 32, 64, 128}) {
                                    builder.add(Arguments.of(blocks,
                                            addTrailingGarbage, incompleteLastBlock,
                                            addGarbageHeader, randomizeGarbage,
                                            corruptEveryAlternateBlock, valueSize));
                                }
                            }
                        }
                    }
                }
            }
        }

        return builder.build();
    }

    @ParameterizedTest
    @MethodSource("provideRecoveryCases")
    void verifyBlockRecoveryWithRandomDataBeforeAndAfter(final int blocks,
            final boolean addTrailingGarbage, final boolean incompleteLastBlock,
            final boolean addGarbageHeader, final boolean randomizeGarbage,
            final boolean corruptEveryAlternateBlock, final int valueSize)
            throws IOException, InterruptedException {
        final ByteArrayOutputStream expectedBlock = new ByteArrayOutputStream();

        final Path tempPath = Files.createTempFile("stormdb_", "_block_util");
        final File tempFile = tempPath.toFile();
        tempFile.deleteOnExit();

        final int recordSize = valueSize + StormDB.KEY_SIZE;
        final int blockSize = StormDB.RECORDS_PER_BLOCK * recordSize
                + StormDB.CRC_SIZE + recordSize;

        try (final FileOutputStream out = new FileOutputStream(tempFile)) {
            final Buffer buffer = new Buffer(valueSize, false);
            for (int i = 0; i < blocks; i++) {
                final byte[] value = new byte[valueSize];
                ThreadLocalRandom.current().nextBytes(value);
                buffer.add(1, value, 0);
                buffer.flush(expectedBlock);
                buffer.clear();
            }

            if (addGarbageHeader) {
                final byte[] garbage = new byte[28];

                if (randomizeGarbage) {
                    ThreadLocalRandom.current().nextBytes(garbage);
                }

                out.write(garbage);
            }
            if (incompleteLastBlock) {
                if (expectedBlock.toByteArray().length > 0) {
                    out.write(expectedBlock.toByteArray(), 0,
                            expectedBlock.toByteArray().length - blockSize / 2);
                }
            } else {
                out.write(expectedBlock.toByteArray());
            }
            if (addTrailingGarbage) {
                final byte[] garbage = new byte[3000];

                if (randomizeGarbage) {
                    ThreadLocalRandom.current().nextBytes(garbage);
                }

                out.write(garbage);
            }

            out.flush();
        }

        // Sleep to let the kernel flush data. If we don't do this,
        // then some tests fail sporadically
        Thread.sleep(10);

        final File recovered = BlockUtil.verifyBlocks(tempFile, valueSize);

        if ((!addGarbageHeader && !addTrailingGarbage && !incompleteLastBlock)
                || (blocks == 0 && incompleteLastBlock && !addTrailingGarbage
                && !addGarbageHeader)) {
            assertSame(tempFile, recovered);
        } else {
            assertNotSame(tempFile, recovered);
        }

        final String message = String.format("blocks=%d, addTrailingGarbage=%s, "
                        + "incompleteLastBlock=%s, addGarbageHeader=%s, "
                        + "randomizeGarbage=%s",
                blocks, addTrailingGarbage,
                incompleteLastBlock, addGarbageHeader,
                randomizeGarbage);

        // TODO: 16/07/2020 this appears to be flaky when running it repeatedly
        final byte[] actual = Files.readAllBytes(recovered.toPath());
        if (incompleteLastBlock) {
            final byte[] expectedBytes;
            expectedBytes = new byte[Math.max(blockSize * (blocks - 1), 0)];
            System.arraycopy(expectedBlock.toByteArray(), 0, expectedBytes, 0,
                    expectedBytes.length);
            assertArrayEquals(expectedBytes, actual, message);
        } else {
            assertArrayEquals(expectedBlock.toByteArray(), actual,
                    message);
        }
    }
}