package com.mode;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.voltdb.client.*;
import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

public class BenchmarkTaxi {
    private enum DataType {
        INTEGER, DOUBLE, STRING, TIMESTAMP
    }

    private static AtomicLong rowId = new AtomicLong(0);

    private final static DateTimeFormatter dtFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    private final static Map<String, DataType> schema = new LinkedHashMap<String, DataType>() {{
        put("vendor_id", DataType.INTEGER);
        put("pickup_datetime", DataType.TIMESTAMP);
        put("dropoff_datetime", DataType.TIMESTAMP);
        put("passenger_count", DataType.INTEGER);
        put("trip_distance", DataType.DOUBLE);
        put("rate_code_id", DataType.INTEGER);
        put("store_and_fwd_flag", DataType.STRING);
        put("dropoff_longitude", DataType.DOUBLE);
        put("dropoff_latitude", DataType.DOUBLE);
        put("payment_type", DataType.INTEGER);
        put("fare_amount", DataType.DOUBLE);
        put("extra", DataType.DOUBLE);
        put("mta_tax", DataType.DOUBLE);
        put("tip_amount", DataType.DOUBLE);
        put("tolls_amount", DataType.DOUBLE);
        put("improvement_surcharge", DataType.DOUBLE);
        put("total_amount", DataType.DOUBLE);
    }};

    public static void main(String[] args) throws IOException, InterruptedException, ProcCallException {
        ClientConfig config = new ClientConfig();

        config.setTopologyChangeAware(true);
        config.setReconnectOnConnectionLoss(true);
        Client voltClient = ClientFactory.createClient(config);
        ExecutorService ingestExecutor = Executors.newFixedThreadPool(24);

        long connectStartTime = System.currentTimeMillis();
//        voltClient.createConnection("localhost", 21212);
        voltClient.createConnection("ip-10-77-2-149.us-west-2.compute.internal", 21212);
        voltClient.createConnection("ip-10-77-2-77.us-west-2.compute.internal", 21212);
        voltClient.createConnection("ip-10-77-2-154.us-west-2.compute.internal", 21212);

        long connectionEndTime = System.currentTimeMillis();

        System.out.println("Total open time: " + (connectionEndTime - connectStartTime) + "ms");

        long clearStartTime = System.currentTimeMillis();
        clearTable(voltClient, "trips");
        long clearEndTime = System.currentTimeMillis();

        System.out.println("Total clear time: " + (clearEndTime - clearStartTime) + "ms");

        long ingestStartTime = System.currentTimeMillis();

        for (String csvFilePath : getCsvPaths()) {
            ingestExecutor.submit(() -> {
                try {
                    System.out.println("Loading " + csvFilePath + " ...");
                    ingestCsvFile(voltClient, schema, "trips", csvFilePath);
                } catch (IOException|InterruptedException ioException) {
                    throw new RuntimeException("Cannot ingest csv file " + csvFilePath, ioException);
                }
            });
        }

        ingestExecutor.shutdown();
        ingestExecutor.awaitTermination(30, TimeUnit.MINUTES);

        long ingestEndTime = System.currentTimeMillis();

        System.out.println("Total ingest time: " + (ingestEndTime - ingestStartTime) + "ms");

        long tableStartTime = System.currentTimeMillis();
        executeTableQuery(voltClient, "trips");
        long tableEndTime = System.currentTimeMillis();

        System.out.println("Offset time (warmup): " + (tableEndTime - tableStartTime) + "ms");

        tableStartTime = System.currentTimeMillis();
        executeTableQuery(voltClient, "trips");
        tableEndTime = System.currentTimeMillis();

        System.out.println("Offset time (warmed): " + (tableEndTime - tableStartTime) + "ms");

        long pivotStartTime = System.currentTimeMillis();
        executePivotQuery(voltClient, "trips");
        long pivotEndTime = System.currentTimeMillis();

        System.out.println("Pivot time (warmup): " + (pivotEndTime - pivotStartTime) + "ms");

        pivotStartTime = System.currentTimeMillis();
        executePivotQuery(voltClient, "trips");
        pivotEndTime = System.currentTimeMillis();

        System.out.println("Pivot time (warmed): " + (pivotEndTime - pivotStartTime) + "ms");

        // Clean up
        voltClient.close();
    }

    /**
     * Setup
     */

    private static ClientResponse clearTable(Client voltClient, String tableName) throws IOException, ProcCallException {
        String deletetSql = "DELETE FROM " + tableName;
        System.out.println(deletetSql);
        return voltClient.callProcedure("@AdHoc", deletetSql);
    }

    /**
     * Ingestion
     */

    private static Double toDouble(String value) {
        return value == null ? null : Double.parseDouble(value);
    }

    private static Integer toInteger(String value) {
        return value == null ? null : Math.round(Float.parseFloat(value));
    }

    private static Long toTimestamp(String dtString) {
        return dtFormat.parseDateTime(dtString).getMillis() * 1000L;
    }

    private static List<String> getCsvPaths() throws IOException {
        final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:**/yellow*{2017,2018}*.csv.gz");

        ArrayList<String> filePaths = new ArrayList<>();
        Files.walkFileTree(Paths.get("data/"), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (matcher.matches(file)) {
                    filePaths.add(file.toString());
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }
        });

        return filePaths;
    }

    private static class LoadFailureCallback implements BulkLoaderFailureCallBack {
        @Override
        public void failureCallback(Object rowHandle, Object[] fieldList, ClientResponse response) {
            System.out.println("Failed to insert row " + rowHandle + " " + response.getStatusString());
        }
    }

    private static void ingestCsvFile(Client voltClient, Map<String, DataType> schema, String tableName, String csvFilePath) throws IOException, InterruptedException {
        VoltBulkLoader loader;

        try {
            Integer batchSize = 1024;
            loader = voltClient.getNewBulkLoader(tableName, batchSize, new LoadFailureCallback());
        } catch (Exception e) {
            throw new RuntimeException("Could not create new bulk loader", e);
        }

        /// Setup Parser
        final CsvParserSettings settings = new CsvParserSettings();
        settings.setHeaderExtractionEnabled(true);
        settings.setMaxCharsPerColumn(-1);

        final CsvParser inputParser = new CsvParser(settings);

        // Begin Parsing
        final File csvFile = new File(csvFilePath);
        final InputStream fileInputStream = new FileInputStream(csvFile);
        final GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
        final BufferedReader csvReader = new BufferedReader(new InputStreamReader(gzipInputStream));

        inputParser.beginParsing(csvReader);

        Integer colNum = 0;
        Integer partMin = 1;
        Integer partCount = 192;
        Random partRandom = new Random();

        String[] csvRow;
        List<Object> voltRow;
        while ((csvRow = inputParser.parseNext()) != null) {
            voltRow = new ArrayList<>(schema.size());
            Iterator colIter = schema.entrySet().iterator();

            // Set Partition
            voltRow.add(rowId.incrementAndGet());
            voltRow.add(partRandom.nextInt((partCount - partMin) + 1) + partMin);

            while (colIter.hasNext()) {
                String rowValue = csvRow[colNum];
                Map.Entry pair = (Map.Entry) colIter.next();

                switch((DataType) pair.getValue()) {
                    case STRING:
                        voltRow.add(rowValue);
                        break;
                    case INTEGER:
                        voltRow.add(toInteger(rowValue));
                        break;
                    case DOUBLE:
                        voltRow.add(toDouble(rowValue));
                        break;
                    case TIMESTAMP:
                        voltRow.add(toTimestamp(rowValue));
                        break;
                }

                colNum += 1;
            }

            colNum = 0;
            loader.insertRow(rowId, voltRow.toArray());
        }

        // Cleanup
        inputParser.stopParsing();

        try {
            loader.drain();
            loader.close();
        } catch (Exception e) {
            throw new RuntimeException("Couldn't close bulk loader", e);
        }
    }


    /**
     * Selection
     */

    private static ClientResponse executeTableQuery(Client voltClient, String tableName) throws IOException, ProcCallException {
        String selectSql = "SELECT id FROM " + tableName + " LIMIT 100 OFFSET 320000";
        System.out.println(selectSql);
        return voltClient.callProcedure("@AdHoc", selectSql);
    }

    private static ClientResponse executePivotQuery(Client voltClient, String tableName) throws IOException, ProcCallException {
        String selectSql =
                "SELECT vendor_id, rate_code_id, COUNT(1) " +
                "FROM " + tableName + " " +
                "GROUP BY vendor_id, rate_code_id " +
                "ORDER BY vendor_id, rate_code_id ";

        System.out.println(selectSql);
        return voltClient.callProcedure("@AdHoc", selectSql);
    }
}
