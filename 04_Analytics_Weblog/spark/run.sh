hadoop fs -rm -R -skipTrash /tmp/weblog/spark/log_excel
chmod +x parse_log_to_excel.py
spark-submit parse_log_to_excel.py