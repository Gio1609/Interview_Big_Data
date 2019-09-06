#!/usr/bin/python
import sys
import re

APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+) ([+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+) "(\S+)" (".+")'

# The below function is modelled specific to Apache Access Logs Model, which can be modified as per needs to different Logs format
# Returns a dictionary containing the parts of the Apache Access Log.
def parse_apache_log_line(logline):
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        raise Error("Invalid logline: %s" % logline)
    
    localIp_or_host = match.group(1)
    req = match.group(7) 
    refer = match.group(11)
    
    print("%s,%s,%s" % (localIp_or_host, req, refer))

# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:
    try:
        parse_apache_log_line(line)
    except ValueError:
        continue
