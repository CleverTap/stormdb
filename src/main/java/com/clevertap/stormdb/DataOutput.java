package com.clevertap.stormdb;

import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by Jude Pereira, at 17:59 on 13/07/2020.
 */
public class DataOutput extends DataOutputStream {
    private long bytesWritten;

    /**
     * Creates a new data output stream to write data to the specified underlying output stream. The
     * counter <code>written</code> is set to zero.
     *
     * @param out the underlying output stream, to be saved for later use.
     * @see FilterOutputStream#out
     */
    public DataOutput(OutputStream out) {
        super(out);
    }

    @Override
    public synchronized void write(int b) throws IOException {
        bytesWritten++;
        super.write(b);
    }

    public long getBytesWritten() {
        return bytesWritten;
    }
}
