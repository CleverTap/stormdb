package com.clevertap.stormdb.maps;

import com.clevertap.stormdb.StormDB;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;

public class DefaultIndexMap implements IndexMap {
    private final TLongLongHashMap indexMap;
    private static final int DEFAULT_INITIAL_CAPACITY = 100_000;
    private static final float DEFAULT_LOAD_FACTOR = 0.95f;

    public DefaultIndexMap() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    public DefaultIndexMap(int initialCapacity, float loadFactor) {
        indexMap = new TLongLongHashMap(initialCapacity, loadFactor, StormDB.RESERVED_KEY_MARKER,
                StormDB.RESERVED_KEY_MARKER);
    }

    @Override
    public void put(long key, long addressValue) {
        indexMap.put(key, addressValue);
    }

    @Override
    public long get(long key) {
        return indexMap.get(key);
    }

    @Override
    public int size() {
        return indexMap.size();
    }
}
