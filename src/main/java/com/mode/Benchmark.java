package com.mode;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class Benchmark {
    private enum DataType {
        INTEGER, FLOAT, STRING, TIMESTAMP
    }

    private final static String csvPath = "data/orders.csv.gz";
    private final static Logger LOGGER = LoggerFactory.getLogger(Benchmark.class);
    private final static DateTimeFormatter dtFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    private final static Map<String, DataType> schema = new LinkedHashMap<String, DataType>() {{
        put("row_number", DataType.INTEGER);
        put("order_id", DataType.INTEGER);
        put("order_gloss_qty", DataType.INTEGER);
        put("order_gloss_amt_usd", DataType.FLOAT);
        put("order_poster_qty", DataType.INTEGER);
        put("order_poster_amt_usd", DataType.FLOAT);
        put("order_standard_qty", DataType.INTEGER);
        put("order_standard_amt_usd", DataType.FLOAT);
        put("order_total_qty", DataType.INTEGER);
        put("order_total_amt_usd", DataType.FLOAT);
        put("order_created_date", DataType.TIMESTAMP);
        put("order_created_year", DataType.INTEGER);
        put("order_created_quarter", DataType.INTEGER);
        put("order_created_month", DataType.INTEGER);
        put("order_created_month_name", DataType.STRING);
        put("order_created_week", DataType.INTEGER);
        put("order_created_day", DataType.INTEGER);
        put("order_created_do_w", DataType.INTEGER);
        put("order_created_do_w_name", DataType.STRING);
        put("order_created_hour", DataType.INTEGER);
        put("account_id", DataType.INTEGER);
        put("account_lat", DataType.FLOAT);
        put("account_lon", DataType.FLOAT);
        put("account_name", DataType.STRING);
        put("account_website", DataType.STRING);
        put("account_primary_contact", DataType.STRING);
        put("web_event_id", DataType.INTEGER);
        put("web_event_channel", DataType.STRING);
        put("web_event_occurred_date", DataType.TIMESTAMP);
        put("web_event_occurred_year", DataType.INTEGER);
        put("web_event_occurred_quarter", DataType.INTEGER);
        put("web_event_occurred_month", DataType.INTEGER);
        put("web_event_created_occurred_name", DataType.STRING);
        put("web_event_occurred_week", DataType.INTEGER);
        put("web_event_occurred_day", DataType.INTEGER);
        put("web_event_occurred_do_w", DataType.INTEGER);
        put("web_event_occurred_do_w_name", DataType.STRING);
        put("web_event_occurred_hour", DataType.INTEGER);
        put("sales_rep_id", DataType.INTEGER);
        put("sales_rep_name", DataType.STRING);
        put("region_id", DataType.INTEGER);
        put("region_name", DataType.STRING);
    }};

    public static void main(String[] args) throws IOException {
        Connection connection = null;

        try {
            long connectStartTime = System.currentTimeMillis();
            connection = DriverManager.getConnection("jdbc:hsqldb:mem:mymemdb", "SA", "");
            long connectionEndTime = System.currentTimeMillis();

            LOGGER.info("Total open time: " + (connectionEndTime - connectStartTime) + "ms");

            long createStartTime = System.currentTimeMillis();
            createTable(connection, schema);
            long createEndTime = System.currentTimeMillis();

            LOGGER.info("Total create time: " + (createEndTime - createStartTime) + "ms");

            long ingestStartTime = System.currentTimeMillis();
            ingestData(connection, schema);
            long ingestEndTime = System.currentTimeMillis();

            LOGGER.info("Total ingest time: " + (ingestEndTime - ingestStartTime) + "ms");

            long tableStartTime = System.currentTimeMillis();
            executeTableQuery(connection);
            long tableEndTime = System.currentTimeMillis();

            LOGGER.info("Offset time (warmup): " + (tableEndTime - tableStartTime) + "ms");

            tableStartTime = System.currentTimeMillis();
            executeTableQuery(connection);
            tableEndTime = System.currentTimeMillis();

            LOGGER.info("Offset time (warmed): " + (tableEndTime - tableStartTime) + "ms");

            long pivotStartTime = System.currentTimeMillis();
            executePivotQuery(connection);
            long pivotEndTime = System.currentTimeMillis();

            LOGGER.info("Pivot time (warmup): " + (pivotEndTime - pivotStartTime) + "ms");

            pivotStartTime = System.currentTimeMillis();
            executePivotQuery(connection);
            pivotEndTime = System.currentTimeMillis();

            LOGGER.info("Pivot time (warmed): " + (pivotEndTime - pivotStartTime) + "ms");

            // Clean up
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

    private static void createTable(Connection connection, Map<String, DataType> schema) throws SQLException {
        Iterator colIter = schema.entrySet().iterator();
        ArrayList<String> columnDefs = new ArrayList<String>();

        while (colIter.hasNext()) {
            Map.Entry pair = (Map.Entry) colIter.next();

            switch((DataType) pair.getValue()) {
                case INTEGER:
                    columnDefs.add(pair.getKey().toString() + " INTEGER");
                    break;
                case FLOAT:
                    columnDefs.add(pair.getKey().toString() + " FLOAT");
                    break;
                case STRING:
                    columnDefs.add(pair.getKey().toString() + " VARCHAR(1024)");
                    break;
                case TIMESTAMP:
                    columnDefs.add(pair.getKey().toString() + " TIMESTAMP");
                    break;
            }
        }

        String createSql = "CREATE TABLE orders(" + String.join(", ", columnDefs) + ")";

        LOGGER.info(createSql);

        // Create and execute statement
        Statement statement = connection.createStatement();
        ResultSet createResult = statement.executeQuery(createSql);

        statement.close();
        createResult.close();
    }

    private static void executeTableQuery(Connection connection) throws SQLException {
        String selectSql = "SELECT * FROM orders LIMIT 100 OFFSET 320000";

        LOGGER.info(selectSql);

        Statement statement = connection.createStatement();
        ResultSet selectResult = statement.executeQuery(selectSql);

        statement.close();
        selectResult.close();
    }

    private static void executePivotQuery(Connection connection) throws SQLException {
        String selectSql =
                "SELECT region_name, sales_rep_name, COUNT(1) " +
                        "FROM orders " +
                        "GROUP BY region_name, sales_rep_name " +
                        "ORDER BY region_name, sales_rep_name";

        LOGGER.info(selectSql);

        Statement statement = connection.createStatement();
        ResultSet selectResult = statement.executeQuery(selectSql);

        statement.close();
        selectResult.close();
    }

    private static void ingestData(Connection connection, Map<String, DataType> schema) throws IOException, SQLException {
        // Build Parameterized Insert
        ArrayList<String> paramDefs = new ArrayList<String>();
        Iterator paramIter = schema.entrySet().iterator();

        while (paramIter.hasNext()) {
            paramIter.next();
            paramDefs.add("?");
        }

        String paramSql = "INSERT INTO orders VALUES(" + String.join(",", paramDefs)+ ")";

        LOGGER.info(paramSql);
        PreparedStatement statement = connection.prepareStatement(paramSql);

        /// Setup Parser
        final CsvParserSettings settings = new CsvParserSettings();
        settings.setHeaderExtractionEnabled(true);
        settings.setMaxCharsPerColumn(-1);

        final CsvParser inputParser = new CsvParser(settings);

        String[] row;
        Integer colNum = 0;
        Integer rowNum = 0;
        Integer batchSize = 325000;

        // Begin Parsing
        final File csvFile = new File(csvPath);
        final InputStream fileInputStream = new FileInputStream(csvFile);
        final GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
        final BufferedReader csvReader = new BufferedReader(new InputStreamReader(gzipInputStream));

        inputParser.beginParsing(csvReader);

        while ((row = inputParser.parseNext()) != null) {
            Iterator colIter = schema.entrySet().iterator();

            while (colIter.hasNext()) {
                String rowValue = row[colNum];
                Map.Entry pair = (Map.Entry) colIter.next();

                switch((DataType) pair.getValue()) {
                    case STRING:
                        statement.setString(colNum + 1, rowValue);
                        break;
                    case INTEGER:
                        statement.setInt(colNum + 1, toInteger(rowValue));
                        break;
                    case FLOAT:
                        statement.setFloat(colNum + 1, toFloat(rowValue));
                        break;
                    case TIMESTAMP:
                        statement.setTimestamp(colNum + 1, toTimestamp(rowValue));
                        break;
                }

                colNum += 1;
            }

            statement.addBatch();

            if (rowNum % batchSize == 0) {
                statement.executeBatch();
            }

            colNum = 0;
            rowNum += 1;
        }

        // Cleanup
        inputParser.stopParsing();
        statement.executeBatch();
        statement.close();
    }

    private static Float toFloat(String fString) {
        return Float.parseFloat(fString);
    }

    private static Integer toInteger(String fString) {
        return Math.round(toFloat(fString));
    }

    private static Timestamp toTimestamp(String dtString) {
        return new Timestamp(dtFormat.parseDateTime(dtString).getMillis() / 1000L);
    }
}
