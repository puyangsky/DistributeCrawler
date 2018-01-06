# !/usr/bin/python
# -*- coding:utf-8 -*-

from kazoo.client import KazooClient
import time

import logging

logging.basicConfig()

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()


@zk.DataWatch('/path/to/watch')
def my_func(data, stat):
    if data:
        print "Data is %s" % data
        print "Version is %s" % stat.version
    else:
        print "data is not a vailable"


while True:
    time.sleep(0.01)
#
# zk.stop()
