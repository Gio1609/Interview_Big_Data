#!/bin/bash
# Run hadoop on Task 0

hadoop fs -rm -R skipTrah /wordcount/task2
chmod +x map.py reduce.py
hadoop jar $HADOOP_HOME/lib/tools/hadoop-streaming-2.9.2.jar \
-D mapreduce.job.reduces=1 \
-file map.py \
-mapper map.py \
-file reduce.py \
-reducer reduce.py \
-input /user/local/temp/wordcount_data \
-output /wordcount/task2

# or input, output from local
#-input file:///user/lhphong/Programming-Assignment-2/articles
#-output file:///user/lhphong/Programming-Assignment-2/articles