# !/usr/bin/python
# -*- coding:utf-8 -*-

import sys

url_map = {}

with open("data/xmly.dat", "r") as f:
    while True:
        line = f.readline()
        if line == "":
            break
        line = line.strip('\n')
        items = line.split("\t")
        if len(items) != 2:
            print >> sys.stderr, "wrong format line: %s" % line
            continue
        url = items[0]
        if url in url_map.keys():
            url_map[url] = url_map[url] + 1
        else:
            url_map[url] = 1
    with open("data/reduplicated_url.dat", "a+") as f2:
        count = 0
        for key, value in url_map.items():
            if value > 1:
                f2.write("%s\n" % key)
                count += 1
        print count