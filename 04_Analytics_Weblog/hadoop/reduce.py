#!/usr/bin/python
import sys

# Parse file logs into file log.csv l
print("localIp_or_host,remote_log_name,remote_user,date,time,local_time,method,req,protocol,http_status_code,size,refer,user_agent")

for line in sys.stdin:
    print(line.strip())

