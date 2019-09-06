#!/usr/bin/python
import sys


# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:

    localIp_or_host = line.strip().split(" ", 1)[0]

    print(localIp_or_host)
    
