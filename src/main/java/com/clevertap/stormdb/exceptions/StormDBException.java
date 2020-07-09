package com.clevertap.stormdb.exceptions;

/**
 * Created by Jude Pereira, at 18:15 on 09/07/2020.
 */
public class StormDBException extends Exception {

    public StormDBException() {
    }

    public StormDBException(String message) {
        super(message);
    }

    public StormDBException(String message, Throwable cause) {
        super(message, cause);
    }

    public StormDBException(Throwable cause) {
        super(cause);
    }

    public StormDBException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
