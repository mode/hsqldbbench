CREATE TABLE trips (
  id BIGINT NOT NULL,
  part SMALLINT NOT NULL,
  vendor_id INTEGER,
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  passenger_count INTEGER,
  trip_distance FLOAT,
  rate_code_id INTEGER,
  store_and_fwd_flag VARCHAR(1),
  dropoff_longitude FLOAT,
  dropoff_latitude FLOAT,
  payment_type INTEGER,
  fare_amount FLOAT,
  extra FLOAT,
  mta_tax FLOAT,
  tip_amount FLOAT,
  tolls_amount FLOAT,
  improvement_surcharge FLOAT,
  total_amount FLOAT,
  PRIMARY KEY (id, part)
);

PARTITION TABLE trips ON COLUMN part;
