package com.clevertap.stormdb;

import com.clevertap.stormdb.utils.ByteUtil;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayDeque;
import java.util.zip.CRC32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities to recover blocks from a corrupted file.
 */
public class BlockUtil {

    private BlockUtil() {
    }

    private static final Logger LOG = LoggerFactory.getLogger(BlockUtil.class);

    private static File rewriteBlocks(final File dirty, final int valueSize) throws IOException {
        LOG.info("Attempting to recover data in {}", dirty);
        final int recordSize = valueSize + StormDB.KEY_SIZE;
        final File newFile = new File(dirty.getParentFile(), dirty.getName() + ".recovered");

        final byte[] syncMarker = Buffer.getSyncMarker(valueSize);
        long blocksRecovered = 0;

        try (final RandomAccessFile in = new RandomAccessFile(dirty, "r");
                final DataOutputStream out = new DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(newFile, false)))) {
            final ArrayDeque<Byte> syncMarkerBuffer = new ArrayDeque<>(syncMarker.length);

            final CRC32 crc32 = new CRC32();

            while (true) {
                if (syncMarkerBuffer.size() == syncMarker.length) {
                    // We're pointing to a sync marker - verify.
                    if (ByteUtil.arrayEquals(syncMarker, syncMarkerBuffer)) {
                        // Start calculating the crc for the next N bytes, and verify.
                        final byte[] data = new byte[StormDB.RECORDS_PER_BLOCK * recordSize];

                        if (in.read(data) != data.length) {
                            // Corrupted.
                            // We touched EOF, signalling a partial write.
                            // The last block was unfortunately lost.
                            break;
                        }
                        crc32.reset();
                        crc32.update(data);

                        // Validate CRC32.
                        if (in.readInt() != (int) crc32.getValue()) {
                            // Rewind the file so that we find the next sync marker.
                            // Rewind such that we can find the next sync marker starting
                            // from the first byte of the last sync marker.
                            in.seek(in.getFilePointer() - data.length - syncMarker.length - 4 + 1);
                            syncMarkerBuffer.clear();
                            continue;
                        }


                        out.write(syncMarker);
                        out.write(data);
                        out.writeInt((int) (crc32.getValue()));
                        syncMarkerBuffer.clear();
                        blocksRecovered++;
                    } else {
                        // Misalignment of the data in the middle of the file.
                        syncMarkerBuffer.removeFirst();
                    }
                } else {
                    int data = in.read();
                    if (data == -1) {
                        // No more data to be read!
                        break;
                    }
                    syncMarkerBuffer.add((byte) (data & 0xFF));
                }
            }
            
            out.flush();
        }

        LOG.info("Successfully recovered {} block(s) from {}", blocksRecovered, dirty);

        return newFile;
    }

    /**
     * Ensures that all blocks in the data and the WAL file match their CRC32 checksums. If they
     * don't, reconstruct the file with only valid blocks.
     * <p>
     * If the data file is reconstructed, it will also replace that file atomically.
     *
     * @return Either the same file (if intact), otherwise a different file containing recovered
     * data.
     */
    static File verifyBlocks(final File dirty, final int valueSize) throws IOException {
        if (!dirty.exists() || dirty.length() == 0) {
            return dirty;
        }

        final int recordSize = valueSize + StormDB.KEY_SIZE;

        final byte[] syncMarker = Buffer.getSyncMarker(valueSize);

        boolean corrupted = false;

        try (final DataInputStream in = new DataInputStream(new BufferedInputStream(
                new FileInputStream(dirty)))) {

            final ArrayDeque<Byte> syncMarkerBuffer = new ArrayDeque<>(syncMarker.length);

            long validBlocks = 0;

            final CRC32 crc32 = new CRC32();

            while (true) {
                if (syncMarkerBuffer.size() == syncMarker.length) {
                    // We're pointing to a sync marker - verify.
                    if (ByteUtil.arrayEquals(syncMarker, syncMarkerBuffer)) {
                        // Start calculating the crc for the next N bytes, and verify.
                        final byte[] data = new byte[StormDB.RECORDS_PER_BLOCK * recordSize];

                        if (in.read(data) != data.length) {
                            // Corrupted.
                            corrupted = true;
                            break;
                        }
                        crc32.reset();
                        crc32.update(data);

                        // Validate CRC32.
                        if (in.readInt() != (int) crc32.getValue()) {
                            corrupted = true;
                            break;
                        }

                        validBlocks++;
                        syncMarkerBuffer.clear();
                    } else {
                        corrupted = true;
                        break;
                    }
                } else {
                    int data = in.read();
                    if (data == -1) {
                        // No more data to be read!
                        break;
                    }
                    syncMarkerBuffer.add((byte) (data & 0xFF));
                }
            }

            // Validate blocks with file size.
            if (!corrupted) {
                final long expectedSize = validBlocks * recordSize * StormDB.RECORDS_PER_BLOCK
                        + (validBlocks * (StormDB.CRC_SIZE + recordSize));
                if (expectedSize != dirty.length()) {
                    corrupted = true;
                }
            }
        }

        if (corrupted) {
            final File reconstructed = rewriteBlocks(dirty, valueSize);
            Files.move(reconstructed.toPath(), dirty.toPath(),
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);

            return new File(dirty.getAbsolutePath());
        }

        return dirty;
    }
}
