# !/usr/bin/python
# -*- coding:utf-8 -*-

from kazoo.client import KazooClient
import time
import logging
import uuid

logging.basicConfig()


def create_ephemeral_node():
    zk = KazooClient(hosts='127.0.0.1:2181')
    zk.start()
    path = "/path/to/watch"
    if zk.exists(path):
        zk.delete(path)
    value = "data_%s" % str(uuid.uuid4())
    zk.create(path, ephemeral=True, value=value)
    time.sleep(3)
    print("create node at %s, value: %s, zk exit..." % (path, value))
    zk.stop()


while True:
    create_ephemeral_node()
    time.sleep(5)