package com.mode;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import java.io.*;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.*;

public class PagingIndex {
    private final ConcurrentSkipListMap<Long, Set<Long>> index;

    public PagingIndex(ConcurrentSkipListMap<Long, Set<Long>> index) {
        this.index = index;
    }

    public Integer size() {
        return index.size();
    }

    public Set<Long> get(Long key) {
        return index.get(key);
    }

    public NavigableSet<Long> keys() {
        return index.keySet();
    }

    public static PagingIndex build(Client voltClient, String tableName, Integer tableParts, String tableColumnName) throws InterruptedException {
        long constructStartTime = System.currentTimeMillis();

        ConcurrentSkipListMap<Long, Set<Long>> index =
                constructIndex(voltClient, tableName, tableParts, tableColumnName);

        long constructEndTime = System.currentTimeMillis();

        System.out.println("Total construction time: " + (constructEndTime - constructStartTime) + "ms");

        return new PagingIndex(index);
    }

    private static ConcurrentSkipListMap<Long, Set<Long>> constructIndex(Client voltClient, String tableName, Integer tableParts, String tableColumnName) throws InterruptedException {
        ExecutorService indexExecutor = Executors.newFixedThreadPool(96);
        ConcurrentSkipListMap<Long, Set<Long>> index = new ConcurrentSkipListMap<>();

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
                                index.computeIfAbsent(key, k
                                        -> new ConcurrentSkipListSet<>()).add(value);
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

        return index;
    }
}
