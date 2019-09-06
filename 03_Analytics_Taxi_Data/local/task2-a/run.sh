#!/bin/bash
# Run hadoop on Task 2a

hadoop fs -rm -R skipTrah /tmp/manage_taxi/task3.2-a
chmod +x map.py reduce.py
hadoop jar $HADOOP_HOME/lib/tools/hadoop-streaming-2.9.2.jar \
-D mapreduce.job.reduces=1 \
-file map.py \
-mapper map.py \
-file reduce.py \
-reducer reduce.py \
-input /tmp/manage_taxi/task3.1/part-00000 \
-output /tmp/manage_taxi/task3.2-a
