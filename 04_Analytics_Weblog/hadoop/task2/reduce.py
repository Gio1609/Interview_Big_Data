#!/usr/bin/python
import sys

current_ip = None
current_req = None
#current_refer = None
list_request = []


for line in sys.stdin:

    logs = line.split(",")
    ip = logs[0].strip()
    req = logs[1].strip()
    refer = logs[2].strip()

    if ip == current_ip:
        if current_req not in list_request:
            list_request.append(current_req)
    else:
        if current_req:
            list_request.append(current_req)
            print("IP: %s       VALUE: %s" % (current_ip, list_request))
            list_request = []

    current_ip = ip
    current_req = req

print("IP: %s       VALUE: %s" % (current_ip, list_request))






    