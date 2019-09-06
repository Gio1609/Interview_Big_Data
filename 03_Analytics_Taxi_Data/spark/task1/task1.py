from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# join data from taxi datas of 2days
if __name__ == "__main__":

    #conf = SparkConf().setAppName("Join Data").setMaster("local[*]")
    sc = SparkContext()

    spark = SparkSession(sc)

    # Read Input Data
    trips = spark.read.csv('/user/local/temp/taxi_data/data_for_2days/trips_*.csv', header=True, inferSchema=True)
    fares = spark.read.csv('/user/local/temp/taxi_data/data_for_2days/fares_*.csv', header=True, inferSchema=True)

    trips.registerTempTable('strip_2day')
    fares.registerTempTable('fare_2day')

    sql_join = '''
        SELECT fare_2day.medallion,
            fare_2day.hack_license,
            fare_2day.vendor_id,
            rate_code,
            store_and_fwd_flag,
            fare_2day.pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_time_in_secs,
            trip_distance,
            pickup_longitude,
            pickup_latitude,
            dropoff_longitude,
            dropoff_latitude,
            payment_type,
            fare_amount,
            surcharge,
            mta_tax,
            tip_amount,
            tolls_amount,
            total_amount
        FROM fare_2day INNER JOIN strip_2day
        ON fare_2day.medallion = strip_2day.medallion
        AND fare_2day.hack_license = strip_2day.hack_license
        AND fare_2day.vendor_id = strip_2day.vendor_id
        AND fare_2day.pickup_datetime = strip_2day.pickup_datetime
    '''

    output_df = spark.sql(sql_join)

    #output_df.show()

    # Write Data
    output_df.coalesce(1).write.csv('hdfs:///tmp/manage_taxi/spark/task3.1',header=True)
    # or output file json
    # output_df.write.json('hdfs:///tmp/manage_taxi/spark/task3.1')
