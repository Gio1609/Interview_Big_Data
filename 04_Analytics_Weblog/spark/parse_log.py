import re
from pyspark.sql import Row

ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+) ([+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+) "(\S+)" (".+")'

def parse_access_log(logline):
    match = re.search(ACCESS_LOG_PATTERN, logline)
    if match is None:
        raise Error("Invalid logline: %s" % logline)

    #fields = [localIp_or_host, remote_log_name, remote_user, date, time, local_time, method, req, protocol, http_status_code, size, refer, user_agent]

    return Row(
        localIp_or_host = match.group(1),
        remote_log_name = match.group(2),
        remote_user = match.group(3),
        date = (match.group(4)).split(":", 1)[0],
        time = (match.group(4)).split(":", 1)[1],
        local_time = match.group(5),
        method = match.group(6),
        req = match.group(7), 
        protocol = match.group(8),
        http_status_code = match.group(9),
        size = match.group(10),
        refer = match.group(11),
        user_agent = match.group(12)
    )