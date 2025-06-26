package com.clevertap.stormdb.maps;

import com.clevertap.stormdb.StormDB;

public interface IndexMap {

    /**
     * API to support put for the key/address pair in question. Key {@link StormDB#RESERVED_KEY_MARKER}
     * is reserved and custom implementations must make sure reserved keys are not used.
     * @param key The key to be inserted
     * @param address The index mapping for the key
     */
    void put(long key, long address);

    /**
     * API to support for get for the key. If get fails, return {@link StormDB#RESERVED_KEY_MARKER}
     * which represents null or not found.
     * @param key The key whose address value is to be retrieved.
     * @return The address value for the key asked.
     */
    long get(long key);


    /**
     * @return Size of the index.
     */
    int size();
}
