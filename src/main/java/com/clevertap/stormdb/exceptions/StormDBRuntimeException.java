package com.clevertap.stormdb.exceptions;

/**
 * Created by Jude Pereira, at 13:42 on 10/07/2020.
 */
public class StormDBRuntimeException extends RuntimeException {

    public StormDBRuntimeException() {
    }

    public StormDBRuntimeException(String message) {
        super(message);
    }

    public StormDBRuntimeException(Throwable cause) {
        super(cause);
    }
}
