package com.clevertap.stormdb.entities;

/**
 * Created by Jude Pereira, at 17:08 on 13/07/2020.
 */
public class Pair<K, V> {

    private final K key;
    private final V value;

    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }
}
