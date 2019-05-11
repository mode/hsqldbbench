### Run

```
mvn clean package && java -Xmx8G -server -jar target/hsqldbbench-1.0-SNAPSHOT.jar
```

### Output (HSQLDB)

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

### Output (VoltDB Taxi)

```
Total open time: 109ms
DELETE FROM trips
Total clear time: 81ms
Loading data/yellow_tripdata_2017-03.csv.gz ...
Loading data/yellow_tripdata_2017-10.csv.gz ...
Loading data/yellow_tripdata_2018-05.csv.gz ...
Loading data/yellow_tripdata_2017-06.csv.gz ...
Loading data/yellow_tripdata_2017-04.csv.gz ...
Loading data/yellow_tripdata_2017-12.csv.gz ...
Loading data/yellow_tripdata_2017-01.csv.gz ...
Loading data/yellow_tripdata_2018-10.csv.gz ...
Loading data/yellow_tripdata_2018-01.csv.gz ...
Loading data/yellow_tripdata_2018-09.csv.gz ...
Loading data/yellow_tripdata_2018-12.csv.gz ...
Loading data/yellow_tripdata_2017-09.csv.gz ...
Loading data/yellow_tripdata_2018-03.csv.gz ...
Loading data/yellow_tripdata_2018-11.csv.gz ...
Loading data/yellow_tripdata_2018-04.csv.gz ...
Loading data/yellow_tripdata_2018-02.csv.gz ...
Loading data/yellow_tripdata_2018-06.csv.gz ...
Loading data/yellow_tripdata_2017-11.csv.gz ...
Loading data/yellow_tripdata_2018-08.csv.gz ...
Loading data/yellow_tripdata_2017-08.csv.gz ...
Loading data/yellow_tripdata_2017-02.csv.gz ...
Loading data/yellow_tripdata_2018-07.csv.gz ...
Loading data/yellow_tripdata_2017-07.csv.gz ...
Loading data/yellow_tripdata_2017-05.csv.gz ...
Total ingest time: 134940ms
SELECT id FROM trips LIMIT 100 OFFSET 320000
Offset time (warmup): 1276ms
SELECT id FROM trips LIMIT 100 OFFSET 320000
Offset time (warmed): 997ms
SELECT vendor_id, rate_code_id, COUNT(1) FROM trips GROUP BY vendor_id, rate_code_id ORDER BY vendor_id, rate_code_id 
Pivot time (warmup): 4459ms
SELECT vendor_id, rate_code_id, COUNT(1) FROM trips GROUP BY vendor_id, rate_code_id ORDER BY vendor_id, rate_code_id 
Pivot time (warmed): 4138ms
```

### VoltDB Notes

```
60> SELECT COUNT(1) FROM trips;
C1        
----------
 216301124

(Returned 1 rows in 0.01s)

61> SELECT MIN(part), MAX(part) FROM trips;
C1  C2  
--- ----
  1  720

(Returned 1 rows in 1.45s)

3> CREATE INDEX trip_fare_amount ON trips (fare_amount);
Command succeeded.

(about 10 seconds)

5> SELECT id FROM trips ORDER BY fare_amount DESC LIMIT 100 OFFSET 320000;
ID        
----------
 177324980
 180210568
 179962822
 186375218
 188067713
 190307022
 194443188
 196254526
 201662189
 206903083
 207882180
 209253260
 214113188
   2311334
   1737913
   3366497
   9321799
  10680039
  13136910
  18874568
  20583085
  26077510
  26033690
  27218801
  28354721
  30721955
  33516104
  38667497
  43134591
  44329388
  47297765
  49303575
  53115845
  58852660
  60570513
  60328438
  65603263
  72745891
  79393801
  81172714
  80235702
  82780425
  83924638
  86929348
  86660843
  86191386
  89324630
  89848309
  95169973
  97593215
  99857645
 100733636
 106103999
 106654775
 106359938
 109810059
 113013563
 112490172
 113909313
 120521711
 124057124
 125126987
 128803149
 129998818
 134529057
 135598635
 134767224
 139099828
 138599266
 140354797
 141358504
 145676917
 150969241
 153863781
 155655146
 160227890
 163935226
 171881300
 173251366
 176532669
 175961006
 182483787
 195579680
 205208532
 204925222
 204878570
 211221289
 212438903
   4486035
   5108979
  15133649
  18570301
  18328752
  19809126
  20155407
  27965220
  31552619
  31179124
  33507354
  38349389

(Returned 100 rows in 6.74s)
```