from pyspark import SparkContext, SparkConf
import sys

import parse_log

def toCSVLine(data):
    return ','.join(str(d) for d in data)

if __name__ == "__main__":
    conf = SparkConf().setAppName("Parse Log to Excel File").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    logFiles = "hdfs:////user/local/temp/access_logs"

    access_logs = sc.textFile(logFiles) \
        .map(parse_log.parse_access_log) \
        .map(toCSVLine) \
        .coalesce(1) \
        .saveAsTextFile('hdfs:///tmp/weblog/spark/log_excel')

#print(access_logs.take(1))



