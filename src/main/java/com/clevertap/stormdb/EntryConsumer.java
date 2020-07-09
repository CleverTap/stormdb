package com.clevertap.stormdb;

import java.io.IOException;

/**
 * Created by Jude Pereira, at 14:33 on 09/07/2020.
 */
public interface EntryConsumer {

    void accept(final int key, final byte[] data, final int offset) throws IOException;
}
