# !/usr/bin/python
# -*- coding:utf-8 -*-

from kazoo.client import KazooClient
import logging
logging.basicConfig()

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

# Ensure a path, create if necessary
zk.ensure_path("/test/zk1")

# Determine if a node exists
if zk.exists("/test/zk1/node"):
    print "the node exist"
else:
    # Create a node with data
    zk.create("/test/zk1/node", b"a test value")

# Print the version of a node and its data
data, stat = zk.get("/test/zk1/node")
print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))

# List the children
children = zk.get_children("/test/zk1")
print("There are %s children with names %s" % (len(children), children))

res = zk.delete("/test/zk1/node")
print("Delete /test/zk1/node, res: %r" % res)

zk.create()

zk.stop()