#!/usr/bin/python
#import libary
import sys
import re


# This is the regex which is specific to Apache Access Logs parsing, which can be modified according to different Log formats as per the need
# Example Apache log line:
# 160.98.220.179 - - [01/Jan/2014:00:20:13 -0500] "GET /ds2/dsbrowse.php?customerid=1 HTTP/1.1" 200 3894 "http://203.3.105.78/" "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Media Center PC 6.0)"

# 160.98.220.179 ===>  1.localIp_or_host
# -  ===>   2.remote_log_name
# -  ===>   3. remote_user
# [01/Jan/2014:00:20:13 -0500]  ===>  4.even_time
# "GET /ds2/dsbrowse.php?customerid=1 HTTP/1.1"  ===>  5.request [method => GET,  req => /ds2/dsbrowse.php?customerid=1,  protocol => HTTP/1.1 ]
# 200  ===>   6.http_status_code
# 3894  ===>   7.size
# "http://203.3.105.78/"  ===>  8.refer
# "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; Media Center PC 6.0)" ===>  9.user_agent


#  
APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+) ([+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+) "(\S+)" (".+")'

# The below function is modelled specific to Apache Access Logs Model, which can be modified as per needs to different Logs format
# Returns a dictionary containing the parts of the Apache Access Log.
def parse_apache_log_line(logline):
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        raise Error("Invalid logline: %s" % logline)
    
    localIp_or_host = match.group(1)
    remote_log_name = match.group(2) 
    remote_user = match.group(3)
    #even_time
    date = (match.group(4)).split(":", 1)[0]
    time = (match.group(4)).split(":", 1)[1]
    local_time = match.group(5)
    #request
    method = match.group(6)
    req = match.group(7) 
    protocol = match.group(8)

    http_status_code = match.group(9)
    size = match.group(10)
    
    refer = match.group(11)
    user_agent = match.group(12)
    
    print("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % 
    (localIp_or_host, remote_log_name, remote_user, date, time, local_time, method, req, protocol, http_status_code, size, refer, user_agent))

    #print("%s$,%s$,%s$,%s$,%s$,%s$,%s$,%s$,%s$,%s$,%s$,%s$,%s" % 
    #(localIp_or_host, remote_log_name, remote_user, date, time, local_time, method, req, protocol, http_status_code, size, refer, user_agent))

    #print("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s" % (match.group(1), match.group(2), match.group(3), match.group(4), 
    #match.group(5), match.group(6), match.group(7), match.group(8), match.group(9), match.group(10), match.group(11) ))

# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:
    try:
        parse_apache_log_line(line)
    except ValueError:
        continue
