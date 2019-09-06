#!/usr/bin/python
import sys

current_ip = None
list_ip_max =[]
ip_max = None
count = 0
total_ip = 0
max_access = 0

for line in sys.stdin:

    localIp_or_host = line.strip()

    if current_ip:
        if localIp_or_host != current_ip:
            if count >= 20:
                total_ip += 1
                print("KEY: %s      VALUE: %d" % (current_ip, count))
                if max_access < count:
                    max_access = count
                    ip_max = current_ip
                    list_ip_max = []
                    list_ip_max.append(current_ip)
                elif max_access == count:
                    list_ip_max.append(current_ip)
            count = 1
            current_ip = localIp_or_host
        else:
            count += 1
    else:
        count += 1
        current_ip = localIp_or_host
        
if count >= 20:
    print("KEY: %s      VALUE: %d" % (current_ip, count))
    if max_access < count:
        max_access = count
        ip_max = current_ip
        list_ip_max.clear()
        list_ip_max.append(current_ip)
    elif max_access == count:
        list_ip_max.append(current_ip)


print("\n============================================")
print("Total ip addresses :     %d" % (total_ip))
print("Max access :     %d" % (max_access))
print("List most access ip :        %s" % (list_ip_max))

