#!/bin/bash
# Run hadoop on Task 1

hadoop fs -rm -R skipTrah /tmp/manage_taxi/task3.1
chmod +x map.py reduce.py
hadoop jar $HADOOP_HOME/lib/tools/hadoop-streaming-2.9.2.jar \
-D mapreduce.job.reduces=1 \
-file map.py \
-mapper map.py \
-file reduce.py \
-reducer reduce.py \
-input /user/local/temp/taxi_data/data_for_2days \
-output /tmp/manage_taxi/task3.1