import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
import re

COMMA_DELIMITER = re.compile("\W+")

if __name__ == "__main__":
    conf = SparkConf().setAppName("wordcount").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    lines = sc.textFile("hdfs:///user/local/temp/wordcount_data")
    words = lines.flatMap(lambda line: COMMA_DELIMITER.split(line)) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)

    #wordcounts = words.countByValue()

    #result = wordcounts.items()

    #for word, count in wordcounts.items():
    #    print("{} : {}".format(word, count))

    words.saveAsTextFile("hdfs:///tmp/wordcount/spark/task2")