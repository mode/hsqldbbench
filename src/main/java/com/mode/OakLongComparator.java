package com.mode;

import com.oath.oak.OakComparator;

import java.nio.ByteBuffer;

public class OakLongComparator implements OakComparator<Long> {
    @Override
    public int compareKeys(Long key1, Long key2) {
        return key1.compareTo(key2);
    }

    @Override
    public int compareSerializedKeys(ByteBuffer serializedKey1, ByteBuffer serializedKey2) {
        return compareKeys(
                serializedKey1.getLong(serializedKey1.position()),
                serializedKey2.getLong(serializedKey2.position()));
    }

    public int compareSerializedKeyAndKey(ByteBuffer serializedKey, Long key2) {
        return compareKeys(
                serializedKey.getLong(serializedKey.position()), key2);
    }
}
