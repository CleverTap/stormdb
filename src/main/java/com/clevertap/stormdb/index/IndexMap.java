package com.clevertap.stormdb.index;

public interface IndexMap {

    /**
     * Mapping having this as the key should not be allowed put since it represents null.
     */
    int NO_ENTRY_KEY = Integer.MAX_VALUE;

    /**
     * Mapping having this as the value should be returned when get fails or in essence
     * represents null value index.
     */
    int NO_ENTRY_INDEX_VALUE =  0xffffffff;


    /**
     * API to support put for the key/index pair in question. Key {@link IndexMap#NO_ENTRY_KEY}
     * should be block
     * @param key The key to be inserted
     * @param indexValue The index mapping for the key
     */
    abstract void put(int key, int indexValue);


    /**
     * API to support for get for the key. If get fails, return {@link IndexMap#NO_ENTRY_INDEX_VALUE}
     * @param key The key whose index value is to be retrieved.
     * @return The index value for the key asked.
     */
    abstract int get(int key);


    /**
     * @return Size of the index.
     */
    abstract int size();
}
