#!/bin/bash
# Run hadoop on Task 2

hadoop fs -rm -R skipTrah /tmp/wordcount/task2.2
chmod +x map.py reduce.py
hadoop jar $HADOOP_HOME/lib/tools/hadoop-streaming-2.9.2.jar \
-D mapreduce.job.reduces=1 \
-file map.py \
-mapper map.py \
-file reduce.py \
-reducer reduce.py \
-input /user/local/temp/wordcount_data \
-output /tmp/wordcount/task2.2