package com.mode;

import com.oath.oak.OakSerializer;

import java.nio.ByteBuffer;

public class OakLongSerializer implements OakSerializer<Long> {
    @Override
    public void serialize(Long value, ByteBuffer targetBuffer) {
        targetBuffer.putLong(targetBuffer.position(), value);
    }

    @Override
    public Long deserialize(ByteBuffer byteBuffer) {
        return byteBuffer.getLong(byteBuffer.position());
    }

    @Override
    public int calculateSize(Long value) {
        return Long.BYTES;
    }
}
