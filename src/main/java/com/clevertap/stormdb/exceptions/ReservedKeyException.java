package com.clevertap.stormdb.exceptions;

/**
 * Created by Jude Pereira, at 14:53 on 09/07/2020.
 */
public class ReservedKeyException extends RuntimeException {

    public ReservedKeyException(final int key) {
        super("The key 0x" + Integer.toHexString(key) + " is reserved for internal structures.");
    }
}
