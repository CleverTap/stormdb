package com.clevertap.stormdb.exceptions;

public class BufferFullException extends RuntimeException {
    private final int requiredSize;
    private final int availableSize;

    public BufferFullException(int requiredSize, int availableSize) {
        super("Buffer full: required " + requiredSize + " bytes, available " + availableSize + " bytes");
        this.requiredSize = requiredSize;
        this.availableSize = availableSize;
    }

    public int getRequiredSize() { return requiredSize; }
    public int getAvailableSize() { return availableSize; }
}

