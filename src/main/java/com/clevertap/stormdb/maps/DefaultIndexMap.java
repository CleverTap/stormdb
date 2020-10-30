package com.clevertap.stormdb.maps;

import com.clevertap.stormdb.StormDB;
import gnu.trove.map.hash.TIntIntHashMap;

public class DefaultIndexMap implements IndexMap {
    private final TIntIntHashMap indexMap;
    private static final int DEFAULT_INITIAL_CAPACITY = 100_000;
    private static final float DEFAULT_LOAD_FACTOR = 0.95f;

    public DefaultIndexMap() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    public DefaultIndexMap(int initialCapacity, float loadFactor) {
        indexMap = new TIntIntHashMap(initialCapacity, loadFactor, StormDB.RESERVED_KEY_MARKER,
                StormDB.RESERVED_KEY_MARKER);
    }

    @Override
    public void put(int key, int indexValue) {
        indexMap.put(key, indexValue);
    }

    @Override
    public int get(int key) {
        return indexMap.get(key);
    }

    @Override
    public int size() {
        return indexMap.size();
    }

    @Override
    public int remove(int key) { return indexMap.remove(key); }
}
