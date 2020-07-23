package com.clevertap.stormdb.maps;

import com.clevertap.stormdb.StormDB;

public interface IndexMap {

    /**
     * API to support put for the key/index pair in question. Key {@link StormDB#RESERVED_KEY_MARKER}
     * is reserved and custom implementations must make sure reserved keys are not used.
     * @param key The key to be inserted
     * @param indexValue The index mapping for the key
     */
    void put(int key, int indexValue);


    /**
     * API to support for get for the key. If get fails, return {@link StormDB#RESERVED_KEY_MARKER}
     * which represents null or not found.
     * @param key The key whose index value is to be retrieved.
     * @return The index value for the key asked.
     */
    int get(int key);


    /**
     * @return Size of the index.
     */
    int size();
}
