package com.clevertap.stormdb.index;

import gnu.trove.map.hash.TIntIntHashMap;

public class DefaultIndexMap implements IndexMap {
    private final TIntIntHashMap indexMap;
    private static final int DEFAULT_INITIAL_CAPACITY = 100_000;
    private static final float DEFAULT_LOAD_FACTOR = 0.95f;

    public DefaultIndexMap() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    public DefaultIndexMap(int initialCapacity, float loadFactor) {
        indexMap = new TIntIntHashMap(initialCapacity, loadFactor, IndexMap.NO_ENTRY_KEY,
                IndexMap.NO_ENTRY_INDEX_VALUE);
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
}
