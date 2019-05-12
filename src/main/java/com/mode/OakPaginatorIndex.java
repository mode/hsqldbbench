package com.mode;

import com.oath.oak.*;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OakPaginatorIndex {
    private final OakMap<Long, ByteBuffer> index;

    public OakPaginatorIndex(OakMap<Long, ByteBuffer> index) {
        this.index = index;
    }

    public Integer size() {
        return index.entries();
    }

    public Iterable<Long> keys() {
        Set<Long> keys = new LinkedHashSet<>();

        OakCloseableIterator<Long> keysIterator = index.keysIterator();

        while(keysIterator.hasNext()) {
            keys.add(keysIterator.next());
        }

        keysIterator.close();

        return keys;
    }

    public Iterable<Long> get(Long key) {
        ByteBuffer buf = index.get(key);
        System.out.println(buf.capacity());
        Set<Long> values = new LinkedHashSet<>();
        while(buf.hasRemaining()) {
            values.add(buf.getLong());
        }
        return values;
    }

    public static OakPaginatorIndex build(Client voltClient, String tableName, Integer tableParts, String tableColumnName) throws InterruptedException {
        long constructStartTime = System.currentTimeMillis();

        OakMap<Long, ByteBuffer> index =
                constructIndex(voltClient, tableName, tableParts, tableColumnName);

        long constructEndTime = System.currentTimeMillis();

        System.out.println("Total construction time: " + (constructEndTime - constructStartTime) + "ms");

        return new OakPaginatorIndex(index);
    }

    private static OakMap<Long, ByteBuffer> constructIndex(Client voltClient, String tableName, Integer tableParts, String tableColumnName) throws InterruptedException {
        OakMap<Long, ByteBuffer> oak = new OakMapBuilder<Long, ByteBuffer>()
                .setKeySerializer(new OakLongSerializer())
                .setValueSerializer(new OakByteBufferSerializer())
                .setComparator(new OakLongComparator())
                .setMinKey(Long.MIN_VALUE).build();

        ExecutorService indexExecutor = Executors.newFixedThreadPool(96);

        for (Integer i = 1; i <= tableParts; i++) {
            final Integer partNum = i;
            indexExecutor.submit(() -> {
                String selectSql =
                        "SELECT id, " + tableColumnName +
                        " FROM " + tableName +
                        " WHERE part = " + partNum;

                System.out.println(selectSql);

                try {
                    ClientResponse response = voltClient.callProcedure("@AdHoc", selectSql);

                    if (response.getStatus() == ClientResponse.SUCCESS) {
                        for (VoltTable table : response.getResults()) {
                            while(table.advanceRow()) {
                                Long key = table.getLong(1);
                                Long value = table.getLong(0);
                                ByteBuffer buffer = ByteBuffer.allocate(8).putLong(value);
                                oak.putIfAbsentComputeIfPresent(key, buffer, oakWBuffer -> {
                                    oakWBuffer.putLong(value);
                                });
                            }
                        }
                    } else {
                        System.out.println(response.getStatusString());
                        throw new RuntimeException(response.getStatusString());
                    }
                } catch (IOException| ProcCallException indexException) {
                    System.out.println(indexException.toString());
                    throw new RuntimeException("Couldn't build index", indexException);
                }
            });
        }

        indexExecutor.shutdown();
        indexExecutor.awaitTermination(30, TimeUnit.SECONDS);

        return oak;
    }
}
