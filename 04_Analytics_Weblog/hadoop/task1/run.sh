#!/bin/bash
# Run hadoop on Task 4.1

hadoop fs -rm -R skipTrah /tmp/weblog/task4.1
chmod +x map.py reduce.py
hadoop jar $HADOOP_HOME/lib/tools/hadoop-streaming-2.9.2.jar \
-D mapreduce.job.reduces=1 \
-file map.py \
-mapper map.py \
-file reduce.py \
-reducer reduce.py \
-input /user/local/temp/access_logs/ \
-output /tmp/weblog/task4.1
