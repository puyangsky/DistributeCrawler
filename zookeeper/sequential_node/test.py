# !/usr/bin/python
# -*- coding:utf-8 -*-

from kazoo.client import KazooClient
import time
import logging
import uuid

logging.basicConfig()

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

path = "/test/seq"
zk.create(path, "value", sequence=True)

zk.stop()