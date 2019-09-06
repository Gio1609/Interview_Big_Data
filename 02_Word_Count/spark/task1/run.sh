hadoop fs -rm -R -skipTrash /tmp/wordcount/spark/task2.1/
chmod +x task1.py
spark-submit task1.py
hadoop fs -mv /tmp/wordcount/spark/task2.1/part-00000 /tmp/wordcount/spark/task2.1/result1.txt