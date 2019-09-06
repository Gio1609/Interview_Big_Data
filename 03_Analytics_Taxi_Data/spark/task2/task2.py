from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# query data from fares data with groupBy fare_amount
if __name__ == "__main__":

    sc = SparkContext()
    spark = SparkSession(sc)

    # Read Input Data
    fares = spark.read.csv('/user/local/temp/taxi_data/data_for_1week/fare_data_*.csv', header=True, inferSchema=True)

    fares.registerTempTable('fares_1week')

    sql_join = '''
        SELECT fare_amount AS amount, COUNT(medallion) AS num_trips
        FROM fares_1week
        GROUP BY fare_amount
        ORDER BY num_trips DESC
    '''

    output_df = spark.sql(sql_join)

    #output_df.show()

    # Write Data
    output_df.coalesce(1).write.csv('hdfs:///tmp/manage_taxi/spark/task3.2',header=True)
    # or output file json
    # output_df.write.json('hdfs:///tmp/manage_taxi/spark/task3.1')
