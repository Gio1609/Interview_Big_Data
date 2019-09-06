
-- ======================================================
-- =                  Assignment 1                      =
-- ======================================================

--===================== Queries with Hadoop ==================
-- Put data to HDFS
hadoop fs -mkdir -p /user/local/temp/taxi_data
hdfs dfs -chmod g+w /user/local/temp/taxi_data

hadoop fs -put '/home/lhphong/Desktop/mock_data/taxi_data/fare_data_week1.csv' '/user/local/temp/taxi_data/fare_data_week1.csv'
hadoop fs -put '/home/lhphong/Desktop/mock_data/taxi_data/fares_wednesday.csv' '/user/local/temp/taxi_data/fares_wednesday.csv'
hadoop fs -put '/home/lhphong/Desktop/mock_data/taxi_data/fares_sunday.csv' '/user/local/temp/taxi_data/fares_sunday.csv'
hadoop fs -put '/home/lhphong/Desktop/mock_data/taxi_data/trip_data_week1.csv' '/user/local/temp/taxi_data/trip_data_week1.csv'
hadoop fs -put '/home/lhphong/Desktop/mock_data/taxi_data/trips_sunday.csv' '/user/local/temp/taxi_data/trips_sunday.csv'
hadoop fs -put '/home/lhphong/Desktop/mock_data/taxi_data/trips_wednesday.csv' '/user/local/temp/taxi_data/trips_wednesday.csv'
hadoop fs -put '/home/lhphong/Desktop/mock_data/taxi_data/licenses.csv' '/user/local/temp/taxi_data/licenses.csv'
-- or put a folder 
hadoop fs -put '/home/lhphong/Desktop/mock_data/taxi_data/' '/user/local/temp/taxi_data'


-------------------------------- Basic queries in hdfs -----------------------------------------------
-- create folder
hadoop fs -mkdir /user/saurzcode/dir1 /user/saurzcode/dir2
-- show list data of directory
hadoop fs -ls /user/saurzcode
-- download
hadoop fs -get <hdfs_src> <localdst>
hadoop fs -get /user/saurzcode/dir3/Samplefile.txt /home/
-- cat
hadoop fs -cat /user/saurzcode/dir1/abc.txt
-- copy
hadoop fs -cp /user/saurzcode/dir1/abc.txt /user/saurzcode/dir2
-- copy from local file system to hdfs
hadoop fs -copyFromLocal /home/saurzcode/abc.txt  /user/saurzcode/abc.txt
-- move
hadoop fs -mv /user/saurzcode/dir1/abc.txt /user/saurzcode/dir2
-- rename
hadoop fs -mv oldname newname 
-- remove a file or directory
hadoop fs -rm /user/saurzcode/dir1/abc.txt
hadoop fs -rmr /user/saurzcode/
-- display last few lines of a file
hadoop fs -tail /user/saurzcode/dir1/abc.txt
-- display the aggregate length of a file
hadoop fs -du /user/saurzcode/dir1/abc.txt

--------------------------------------------------------------------------------------------------------



------------------------------------------ Query data in table trip & fare ----------------------------------------
trips = LOAD '/user/local/temp/taxi_data/trip_data_week1.csv' using PigStorage(',') as (medallion:chararray, hack_license:chararray, vendor_id:chararray, rate_code:int, store_and_fwd_flag:chararray, pickup_datetime:datetime, dropoff_datetime:datetime,passenger_count:int,trip_time_in_secs:int,trip_distance:double,pickup_longitude:double,pickup_latitude:double,dropoff_longitude:double,dropoff_latitude:double);

fares = LOAD '/user/local/temp/taxi_data/fare_data_week1.csv' using PigStorage(',') as (medallion:chararray, hack_license:chararray, vendor_id:chararray, pickup_datetime:datetime, payment_type:chararray, fare_amount:double, surcharge:double, mta_tax:double, tip_amount:double, tolls_amount:double, total_amount:double);

-- Task 1: Write a SQL query that joins the 'trips' and 'fare' table and populates a new table called 'alltrips' table. Note that the 'fares' and 'trips' tables share 4 attributes: medallion, hack, vendor_id, pickup datetime.
j1 = JOIN trips BY (medallion, hack_license, vendor_id, pickup_datetime), fares BY (medallion, hack_license, vendor_id, pickup_datetime);
join1 = FOREACH j1 GENERATE trips::medallion, trips::hack_license, trips::vendor_id, trips::pickup_datetime, fares::payment_type, fares::fare_amount, fares::surcharge, fares::total_amount;

-- Left outer join
jleft = JOIN trips BY (medallion, hack_license, vendor_id, pickup_datetime) LEFT OUTER, fares BY (medallion, hack_license, vendor_id, pickup_datetime);
DUMP jleft;
-- Right outer join
jright = JOIN trips BY (medallion, hack_license, vendor_id, pickup_datetime) RIGHT, fares BY (medallion, hack_license, vendor_id, pickup_datetime);
DUMP jright;
-- Full outer join
jfull = JOIN trips BY (medallion, hack_license, vendor_id, pickup_datetime) FULL OUTER, fares BY (medallion, hack_license, vendor_id, pickup_datetime);
DUMP jfull;

-- Task 2: Write SQL queries flowwing
-- Find the distribution of fare amounts, i.e., for each amount A, the number of trips that cost A. The schema of the output should be: (amount, num_trips)
query1 = GROUP fares BY fare_amount;

-----------------------------------------------------------------------------------------------------------

------------------------------------- Queries data in directory Logs file -----------------------------------
-- %h %l %u %t %r %>s %b Refer User_agent
-- localIp_or_host % remote_log_name % remote_user % even_time % request (method_req $ url_req) % http_status_code % size % refer % user_agent

log1 = LOAD '/user/local/temp/access_logs/access_log-20140101' using PigStorage(',') as (localIp_or_host:chararray, remote_log_name:chararray, remote_user:chararray, even_time:chararray, request:chararray, http_status_code:chararray, size:chararray, refer:chararray, user_agent:chararray);



-- ======================================= Queries with Hive ==============================================
CREATE SCHEMA IF NOT EXISTS taxi_data
USE taxi_data 

-- Create table trip_data_week1
DROP TABLE IF EXISTS trip_data_week1;
CREATE EXTERNAL TABLE trip_data_week1 (medallion STRING, hack_license STRING, vendor_id STRING, rate_code INT, store_and_fwd_flag STRING, pickup_datetime TIMESTAMP, dropoff_datetime TIMESTAMP,passenger_count INT, trip_time_in_secs INT, trip_distance DOUBLE, pickup_longitude DOUBLE, pickup_latitude DOUBLE, dropoff_longitude DOUBLE, dropoff_latitude DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA local inpath "/home/lhphong/Desktop/mock_data/taxi_data/trip_data_week1.csv" OVERWRITE INTO TABLE trip_data_week1;

-- Create table trips_sunday
DROP TABLE IF EXISTS trips_sunday;
CREATE EXTERNAL TABLE trips_sunday (medallion STRING, hack_license STRING, vendor_id STRING, rate_code INT, store_and_fwd_flag STRING, pickup_datetime TIMESTAMP, dropoff_datetime TIMESTAMP,passenger_count INT, trip_time_in_secs INT, trip_distance DOUBLE, pickup_longitude DOUBLE, pickup_latitude DOUBLE, dropoff_longitude DOUBLE, dropoff_latitude DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA local inpath "/home/lhphong/Desktop/mock_data/taxi_data/trips_sunday.csv" OVERWRITE INTO TABLE trips_sunday;

-- Create table trips_wednesday
DROP TABLE IF EXISTS trips_wednesday;
CREATE EXTERNAL TABLE trips_wednesday (medallion STRING, hack_license STRING, vendor_id STRING, rate_code INT, store_and_fwd_flag STRING, pickup_datetime TIMESTAMP, dropoff_datetime TIMESTAMP,passenger_count INT, trip_time_in_secs INT, trip_distance DOUBLE, pickup_longitude DOUBLE, pickup_latitude DOUBLE, dropoff_longitude DOUBLE, dropoff_latitude DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA local inpath "/home/lhphong/Desktop/mock_data/taxi_data/trips_wednesday.csv" OVERWRITE INTO TABLE trips_wednesday;

-- Create table fare_data_week1
DROP TABLE IF EXISTS fare_data_week1;
CREATE EXTERNAL TABLE fare_data_week1 (medallion STRING, hack_license STRING, vendor_id STRING, pickup_datetime TIMESTAMP, payment_type STRING, fare_amount DOUBLE, surcharge DOUBLE, mta_tax DOUBLE, tip_amount DOUBLE, tolls_amount DOUBLE, total_amount DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA local inpath "/home/lhphong/Desktop/mock_data/taxi_data/fare_data_week1.csv" OVERWRITE INTO TABLE fare_data_week1;

-- Create table fares_sunday
DROP TABLE IF EXISTS fares_sunday;
CREATE EXTERNAL TABLE fares_sunday (medallion STRING, hack_license STRING, vendor_id STRING, pickup_datetime TIMESTAMP, payment_type STRING, fare_amount DOUBLE, surcharge DOUBLE, mta_tax DOUBLE, tip_amount DOUBLE, tolls_amount DOUBLE, total_amount DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA local inpath "/home/lhphong/Desktop/mock_data/taxi_data/fares_sunday.csv" OVERWRITE INTO TABLE fares_sunday;

-- Create table fares_wednesday
DROP TABLE IF EXISTS fares_wednesday;
CREATE EXTERNAL TABLE fares_wednesday (medallion STRING, hack_license STRING, vendor_id STRING, pickup_datetime TIMESTAMP, payment_type STRING, fare_amount DOUBLE, surcharge DOUBLE, mta_tax DOUBLE, tip_amount DOUBLE, tolls_amount DOUBLE, total_amount DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA local inpath "/home/lhphong/Desktop/mock_data/taxi_data/fares_wednesday.csv" OVERWRITE INTO TABLE fares_wednesday;

-- Create table medallions 
DROP TABLE IF EXISTS medallions ;
CREATE EXTERNAL TABLE medallions (medallion STRING, hack_license STRING, types STRING, current_status STRING, DMV_license_plate STRING, vehicle_VIN_number STRING, vehicle_type STRING, model_year INT, medallion_type STRING, agent_number INT, agent_name STRING, agent_telephone_number STRING, agent_website STRING, agent_address STRING, last_updated_date STRING, last_updated_time STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar"="\"",
"escapeChar"="\\"
)
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");
LOAD DATA local inpath "/home/lhphong/Desktop/mock_data/taxi_data/licenses.csv" OVERWRITE INTO TABLE medallions;


-- Task 1: Write a SQL query that joins the 'trips' and 'fare' table and populates a new table called 'alltrips' table. Note that the 'fares' and 'trips' tables share 4 attributes: medallion, hack, vendor_id, pickup datetime.
CREATE VIEW alltrips AS
SELECT fare_data_week1.medallion,fare_data_week1.hack_license,fare_data_week1.vendor_id,rate_code,store_and_fwd_flag,fare_data_week1.pickup_datetime,dropoff_datetime,passenger_count,trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount
FROM fare_data_week1 INNER JOIN trip_data_week1 
ON fare_data_week1.medallion = trip_data_week1.medallion
AND fare_data_week1.hack_license = trip_data_week1.hack_license
AND fare_data_week1.vendor_id = trip_data_week1.vendor_id
AND fare_data_week1.pickup_datetime = trip_data_week1.pickup_datetime;


-- Task 2:
-- a) Find the distribution of fare amounts, i.e., for each amount A, the number of trips that cost A. The schema of the output should be: (amount, num_trips)

SELECT fare_amount AS amount, COUNT(*) AS num_trips
FROM fares
GROUP BY fare_amount;

-- For a more significant result, which would exclude some anomalies:
-- SELECT fare_amount AS amount, COUNT(*) AS num_trips FROM fares GROUP BY fare_amount HAVING COUNT(*) > 5 AND amount > 0;

-- b) How many trips cost less than $10?

SELECT COUNT(*) AS num_trips
FROM fares
WHERE total_amount < 10;

-- c) Find the distribution of the number of passengers. The schema of the output should be: (number_of_passengers, num_trips)

SELECT passenger_count AS number_of_passengers, COUNT(*) AS num_trips
FROM trips
GROUP BY passenger_count;

-- d) Find the total revenue (for all taxis) per day. The schema of the output should be: (day, total_revenue).

SELECT DATE(pickup_datetime) AS day, SUM(total_amount) AS total_revenue
FROM fares
GROUP BY DATE(pickup_datetime);


-- e) Find the total number of trips for each taxi

SELECT medallion, COUNT(*) AS number_of_trips
FROM trips
GROUP BY medallion;

-- Or this, depending on what uniquely identifies one taxi:
SELECT medallion, hack_license, vendor_id, COUNT(*) AS number_of_trips FROM trips GROUP BY medallion, hack_license, vendor_id;

-- Task 3: 

-- a) Is there more than one record for a given taxi at the same time? If there are any, what's interesting about it?
-- The query below show all taxis that have more than one record with the same pickup time
SELECT medallion, hack_license, vendor_id, pickup_datetime, COUNT(*)
FROM trips
GROUP BY medallion, pickup_datetime
HAVING COUNT(*) > 1;

-- b) For each taxi, what is the percentage of trips without GPS coordinates, i.e., all 4 coordinates are recorded as 0's?
-- The output schema should be: (medallion, percentage_of_trips). 

-- Let's use this index to make the query run faster:
CREATE INDEX trips_geo ON trips (pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude);

SELECT t1.medallion, CONCAT(100 * no_gps_trips_count / all_trips_count, '%') AS percentage_of_trips
FROM ((SELECT medallion, COUNT(*) AS no_gps_trips_count
	   FROM trips
	   WHERE pickup_longitude = 0
     	 AND pickup_latitude = 0
		 AND dropoff_longitude = 0
		 AND dropoff_latitude = 0
	   GROUP BY medallion) AS t1
INNER JOIN (SELECT medallion, COUNT(*) AS all_trips_count
			FROM trips
			GROUP BY medallion) AS t2)
WHERE t1.medallion = t2.medallion;


-- c) Find the number of different taxis used by each driver. Can you identify anything unusual?

SELECT hack_license, COUNT(DISTINCT medallion)
FROM trips
GROUP BY hack_license;

-- Yes, some drivers drove more than 1 taxi during that week.

-- Task 4:














































