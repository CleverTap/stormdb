package com.clevertap.stormdb.exceptions;

/**
 * Created by Jude Pereira, at 12:35 on 15/07/2020.
 */
public class ReadOnlyBufferException extends StormDBRuntimeException {

    public ReadOnlyBufferException(String s) {
        super(s);
    }
}
