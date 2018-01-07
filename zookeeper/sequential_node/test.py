# !/usr/bin/python
# -*- coding:utf-8 -*-

from kazoo.client import KazooClient
import time
import logging
import os

logging.basicConfig()

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

path = "/test/seq"
zk.ensure_path(path)
zk.create(path + "/hello", "value", sequence=True)

children = zk.get_children(path)
print(type(children))
for child in children:
    print(child)
    print(zk.get(os.path.join(path, child)))

zk.stop()