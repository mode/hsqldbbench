package com.mode;

import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.roaringbitmap.RoaringBitmap;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;


public class PagingIndex {
    private final TreeMap<Long, Roaring64NavigableMap> index;

    public PagingIndex(TreeMap<Long, Roaring64NavigableMap> index) {
        this.index = index;
    }

    public Integer size() {
        return index.size();
    }

    public Set<Long> keys() {
        return index.keySet();
    }

    public Roaring64NavigableMap get(Long key) {
        return index.get(key);
    }

    public Set<Long> lookup(Long limit, Long offset) {
        Long seen = 0L;
        Set<Long> result = new LinkedHashSet<>();

        for (Map.Entry<Long, Roaring64NavigableMap> entry : index.entrySet()) {
            if (result.size() >= limit) {
                break;
            }

            Roaring64NavigableMap values = entry.getValue();

            Long cardinality = values.getLongCardinality();

            if (seen + cardinality < offset) {
                seen += cardinality;
                // Skip over this key
            } else {
                Long seek = (offset - seen);
                Long length = cardinality - seek;

                seen += seek;
                for (Long pos = seek; pos < length; pos++) {
                    if (result.size() >= limit) {
                        break;
                    }

                    seen += 1;
                    result.add(values.select(pos));
                }
            }
        }

        return result;
    }


    public static PagingIndex build(Client voltClient, String tableName, Integer tableParts, String tableColumnName) throws InterruptedException, ExecutionException {
        return new PagingIndex(constructIndex(voltClient, tableName, tableParts, tableColumnName));
    }

    private static TreeMap<Long, Roaring64NavigableMap> constructIndex(Client voltClient, String tableName, Integer tableParts, String tableColumnName) throws InterruptedException, ExecutionException {
        ArrayList<Future<Map<Long, Roaring64NavigableMap>>> indices = new ArrayList<>();
        ExecutorService indexExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        for (Integer i = 1; i <= tableParts; i++) {
            final Integer partNum = i;
            indices.add(indexExecutor.submit(() -> {
                String selectSql =
                        "SELECT id, " + tableColumnName +
                        " FROM " + tableName +
                        " WHERE part = " + partNum;

                System.out.println(selectSql);

                try {
                    ClientResponse response = voltClient.callProcedure("@AdHoc", selectSql);

                    if (response.getStatus() == ClientResponse.SUCCESS) {
                        Map<Long, Roaring64NavigableMap> keyMap = new HashMap<>();

                        for (VoltTable table : response.getResults()) {
                            while(table.advanceRow()) {
                                Long key = table.getLong(1);
                                Long value = table.getLong(0);
                                keyMap.computeIfAbsent(key, absentKey
                                        -> new Roaring64NavigableMap()).add(value);
                            }
                        }

                        return keyMap;
                    } else {
                        System.out.println(response.getStatusString());
                        throw new RuntimeException(response.getStatusString());
                    }
                } catch (IOException| ProcCallException indexException) {
                    System.out.println(indexException.toString());
                    throw new RuntimeException("Couldn't build index", indexException);
                }
            }));
        }

        indexExecutor.shutdown();
        indexExecutor.awaitTermination(30, TimeUnit.SECONDS);

        /// Merge all the maps into one
        TreeMap<Long, Roaring64NavigableMap> merged = new TreeMap<>();
        for (Future<Map<Long, Roaring64NavigableMap>> index : indices) {
            for (Map.Entry<Long, Roaring64NavigableMap> entry : index.get().entrySet()) {
                merged.computeIfAbsent(entry.getKey(), absentKey
                        -> new Roaring64NavigableMap()).or(entry.getValue());
            }
        }

        return merged;
    }
}
