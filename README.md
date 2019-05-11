### Run

```
mvn clean package && java -Xmx8G -server -jar target/hsqldbbench-1.0-SNAPSHOT.jar
```

### Output

```
May 09, 2019 9:20:21 AM com.mode.Benchmark main
INFO: Total open time: 463ms
May 09, 2019 9:20:21 AM com.mode.Benchmark createTable
INFO: CREATE TABLE orders(row_number INTEGER, order_id INTEGER, order_gloss_qty INTEGER, order_gloss_amt_usd FLOAT, order_poster_qty INTEGER, order_poster_amt_usd FLOAT, order_standard_qty INTEGER, order_standard_amt_usd FLOAT, order_total_qty INTEGER, order_total_amt_usd FLOAT, order_created_date TIMESTAMP, order_created_year INTEGER, order_created_quarter INTEGER, order_created_month INTEGER, order_created_month_name VARCHAR(1024), order_created_week INTEGER, order_created_day INTEGER, order_created_do_w INTEGER, order_created_do_w_name VARCHAR(1024), order_created_hour INTEGER, account_id INTEGER, account_lat FLOAT, account_lon FLOAT, account_name VARCHAR(1024), account_website VARCHAR(1024), account_primary_contact VARCHAR(1024), web_event_id INTEGER, web_event_channel VARCHAR(1024), web_event_occurred_date TIMESTAMP, web_event_occurred_year INTEGER, web_event_occurred_quarter INTEGER, web_event_occurred_month INTEGER, web_event_created_occurred_name VARCHAR(1024), web_event_occurred_week INTEGER, web_event_occurred_day INTEGER, web_event_occurred_do_w INTEGER, web_event_occurred_do_w_name VARCHAR(1024), web_event_occurred_hour INTEGER, sales_rep_id INTEGER, sales_rep_name VARCHAR(1024), region_id INTEGER, region_name VARCHAR(1024))
May 09, 2019 9:20:21 AM com.mode.Benchmark main
INFO: Total create time: 14ms
May 09, 2019 9:20:21 AM com.mode.Benchmark ingestData
INFO: INSERT INTO orders VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
May 09, 2019 9:20:24 AM com.mode.Benchmark main
INFO: Total ingest time: 3487ms
May 09, 2019 9:20:24 AM com.mode.Benchmark executeTableQuery
INFO: SELECT * FROM orders LIMIT 100 OFFSET 320000
May 09, 2019 9:20:25 AM com.mode.Benchmark main
INFO: Offset time (warmup): 157ms
May 09, 2019 9:20:25 AM com.mode.Benchmark executeTableQuery
INFO: SELECT * FROM orders LIMIT 100 OFFSET 320000
May 09, 2019 9:20:25 AM com.mode.Benchmark main
INFO: Offset time (warmed): 167ms
May 09, 2019 9:20:25 AM com.mode.Benchmark executePivotQuery
INFO: SELECT region_name, sales_rep_name, COUNT(1) FROM orders GROUP BY region_name, sales_rep_name ORDER BY region_name, sales_rep_name
May 09, 2019 9:20:25 AM com.mode.Benchmark main
INFO: Pivot time (warmup): 289ms
May 09, 2019 9:20:25 AM com.mode.Benchmark executePivotQuery
INFO: SELECT region_name, sales_rep_name, COUNT(1) FROM orders GROUP BY region_name, sales_rep_name ORDER BY region_name, sales_rep_name
May 09, 2019 9:20:25 AM com.mode.Benchmark main
INFO: Pivot time (warmed): 292ms
```