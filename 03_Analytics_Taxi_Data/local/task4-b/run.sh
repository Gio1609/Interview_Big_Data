#!/bin/bash
# Run hadoop on Task 4b

hadoop fs -rm -R skipTrah /manage_taxi/task3.4-b
chmod +x map.py reduce.py
hadoop jar $HADOOP_HOME/lib/tools/hadoop-streaming-2.9.2.jar \
-D mapreduce.job.reduces=1 \
-file map.py \
-mapper map.py \
-file reduce.py \
-reducer reduce.py \
-input /manage_taxi/task3.3/part-00000 \
-output /manage_taxi/task3.4-b
