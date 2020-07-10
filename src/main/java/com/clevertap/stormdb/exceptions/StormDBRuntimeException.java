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

    public StormDBRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public StormDBRuntimeException(Throwable cause) {
        super(cause);
    }

    public StormDBRuntimeException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
