package com.mode;

import com.oath.oak.OakSerializer;

import java.nio.ByteBuffer;

public class OakByteBufferSerializer implements OakSerializer<ByteBuffer> {
    @Override
    public void serialize(ByteBuffer value, ByteBuffer targetBuffer) {
        targetBuffer.put(value);
    }

    @Override
    public ByteBuffer deserialize(ByteBuffer byteBuffer) {
        return byteBuffer;
    }

    @Override
    public int calculateSize(ByteBuffer value) {
        return value.capacity();
    }
}
