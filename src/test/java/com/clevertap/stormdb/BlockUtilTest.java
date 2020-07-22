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

    // Create multiple dbConfig for parallel test.
    private final Config dbConfig = new Config(); // Default dbConfig

    @Test
    void verifyBlocksGood() throws IOException {
        dbConfig.valueSize = 100;
        final Buffer buffer = new Buffer(dbConfig, false);
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
            final boolean addGarbageBetweenEveryBlock, final int valueSize)
            throws IOException {
        final ByteArrayOutputStream corruptedFileContent = new ByteArrayOutputStream();
        final ByteArrayOutputStream expectedValidBlocks = new ByteArrayOutputStream();

        final Path tempPath = Files.createTempFile("stormdb_", "_block_util");
        final File tempFile = tempPath.toFile();
        tempFile.deleteOnExit();

        final int recordSize = valueSize + Config.KEY_SIZE;
        final int blockSize = Config.RECORDS_PER_BLOCK * recordSize
                + Config.CRC_SIZE + recordSize;

        try (final FileOutputStream out = new FileOutputStream(tempFile)) {
            if (addGarbageHeader) {
                final byte[] garbage = new byte[28];

                if (randomizeGarbage) {
                    ThreadLocalRandom.current().nextBytes(garbage);
                }

                corruptedFileContent.write(garbage);
            }

            dbConfig.valueSize = valueSize;
            final Buffer buffer = new Buffer(dbConfig, false);
            for (int i = 0; i < blocks; i++) {
                final byte[] value = new byte[valueSize];
                ThreadLocalRandom.current().nextBytes(value);
                buffer.add(1, value, 0);

                buffer.flush(corruptedFileContent);
                buffer.flush(expectedValidBlocks);
                buffer.clear();

                if (addGarbageBetweenEveryBlock) {
                    final byte[] garbage = new byte[blockSize];
                    if (randomizeGarbage) {
                        ThreadLocalRandom.current().nextBytes(garbage);
                    }
                    corruptedFileContent.write(garbage);
                }
            }

            if (incompleteLastBlock) {
                if (corruptedFileContent.toByteArray().length > 0) {
                    final int len = corruptedFileContent.toByteArray().length - blockSize / 2 - (
                            addGarbageBetweenEveryBlock ? blockSize : 0);
                    out.write(corruptedFileContent.toByteArray(), 0, Math.max(len, 0));
                }
            } else {
                final byte[] data = corruptedFileContent.toByteArray();
                out.write(data);
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

        final long corruptedFileLength = tempFile.length();

        final File recovered = BlockUtil.verifyBlocks(tempFile, valueSize);

        if (corruptedFileLength == 0
                || (!addGarbageHeader && !addTrailingGarbage
                && !addGarbageBetweenEveryBlock && !incompleteLastBlock)) {
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

        final byte[] actual = Files.readAllBytes(recovered.toPath());
        if (incompleteLastBlock) {
            final byte[] expectedBytes;
            expectedBytes = new byte[Math.max(blockSize * (blocks - 1), 0)];
            System.arraycopy(expectedValidBlocks.toByteArray(), 0, expectedBytes, 0,
                    expectedBytes.length);
            assertArrayEquals(expectedBytes, actual, message);
        } else {
            assertArrayEquals(expectedValidBlocks.toByteArray(), actual,
                    message);
        }
    }
}