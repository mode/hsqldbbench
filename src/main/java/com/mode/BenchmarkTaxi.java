package com.mode;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class BenchmarkTaxi {
    private enum DataType {
        INTEGER, DOUBLE, STRING, TIMESTAMP
    }

    private final static Logger LOGGER = LoggerFactory.getLogger(Benchmark.class);
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

    public static void main(String[] args) throws IOException, InterruptedException {
        Client voltClient = null;
        Connection connection = null;

        try {
            long connectStartTime = System.currentTimeMillis();

            voltClient = ClientFactory.createClient();
            voltClient.createConnection("localhost", 21212);
            connection = DriverManager.getConnection("jdbc:voltdb://localhost:21212?autoreconnect=true");

            long connectionEndTime = System.currentTimeMillis();

            LOGGER.info("Total open time: " + (connectionEndTime - connectStartTime) + "ms");

            long ingestStartTime = System.currentTimeMillis();

            Long lastRowCount = 0L;
            for (String csvFilePath : getCsvPaths()) {
                lastRowCount = ingestCsvFile(voltClient, schema, "trips", csvFilePath, lastRowCount);
            }

            long ingestEndTime = System.currentTimeMillis();

            LOGGER.info("Total ingest time: " + (ingestEndTime - ingestStartTime) + "ms");

            long tableStartTime = System.currentTimeMillis();
            executeTableQuery(connection, "trips");
            long tableEndTime = System.currentTimeMillis();

            LOGGER.info("Offset time (warmup): " + (tableEndTime - tableStartTime) + "ms");

            tableStartTime = System.currentTimeMillis();
            executeTableQuery(connection, "trips");
            tableEndTime = System.currentTimeMillis();

            LOGGER.info("Offset time (warmed): " + (tableEndTime - tableStartTime) + "ms");

            long pivotStartTime = System.currentTimeMillis();
            executePivotQuery(connection, "trips");
            long pivotEndTime = System.currentTimeMillis();

            LOGGER.info("Pivot time (warmup): " + (pivotEndTime - pivotStartTime) + "ms");

            pivotStartTime = System.currentTimeMillis();
            executePivotQuery(connection, "trips");
            pivotEndTime = System.currentTimeMillis();

            LOGGER.info("Pivot time (warmed): " + (pivotEndTime - pivotStartTime) + "ms");

            // Clean up
            voltClient.close();
            connection.close();
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        } finally {
            try {
                // Close connection
                if (connection != null)
                    connection.close();
            } catch (SQLException e) {
                LOGGER.error(e.getMessage());
            }
        }
    }

    /**
     * Ingestion
     */

    private static class LoadFailureCallback implements BulkLoaderFailureCallBack {
        @Override
        public void failureCallback(Object rowHandle, Object[] fieldList, ClientResponse response) {
            LOGGER.error("Failed to insert row " + rowHandle + " " + response.getStatusString());
        }
    }

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

    private static Long ingestCsvFile(Client voltClient, Map<String, DataType> schema, String tableName, String csvFilePath, Long lastRowCount) throws IOException, InterruptedException {
        Integer batchSize = 325000;
        VoltBulkLoader loader = null;
        try {
            // new SessionBulkloaderFailureCallback();
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

        Long rowCount = 0L;
        Integer colNum = 0;
        Integer partMin = 1;
        Integer partCount = 720;
        Random partRandom = new Random();

        String[] csvRow;
        List<Object> voltRow;
        while ((csvRow = inputParser.parseNext()) != null) {
            Long rowId = rowCount + lastRowCount + 1;
            voltRow = new ArrayList<>(schema.size());
            Iterator colIter = schema.entrySet().iterator();

            // Set Partition
            voltRow.add(rowId);
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
            rowCount += 1;

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

        return rowCount;
    }


    /**
     * Selection
     */

    private static void executeTableQuery(Connection connection, String tableName) throws SQLException {
        String selectSql = "SELECT id FROM " + tableName + " LIMIT 100 OFFSET 320000";

        LOGGER.info(selectSql);

        Statement statement = connection.createStatement();
        ResultSet selectResult = statement.executeQuery(selectSql);

        statement.close();
        selectResult.close();
    }

    private static void executePivotQuery(Connection connection, String tableName) throws SQLException {
        String selectSql =
                "SELECT vendor_id, rate_code_id, COUNT(1)" +
                "FROM " + tableName + " " +
                "GROUP BY vendor_id, rate_code_id" +
                "ORDER BY vendor_id, rate_code_id";

        LOGGER.info(selectSql);

        Statement statement = connection.createStatement();
        ResultSet selectResult = statement.executeQuery(selectSql);

        statement.close();
        selectResult.close();
    }
}
