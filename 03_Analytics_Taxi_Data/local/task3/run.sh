#!/bin/bash
# Run hadoop on Task 3

hadoop fs -rm -R skipTrah /tmp/manage_taxi/task3.3
chmod +x map.py reduce.py
hadoop jar $HADOOP_HOME/lib/tools/hadoop-streaming-2.9.2.jar \
-D mapreduce.job.reduces=1 \
-file map.py \
-mapper map.py \
-file reduce.py \
-reducer reduce.py \
-input /tmp/manage_taxi/task3.1/part-00000 /user/local/temp/taxi_data/vehicle_data \
-output /tmp/manage_taxi/task3.3
