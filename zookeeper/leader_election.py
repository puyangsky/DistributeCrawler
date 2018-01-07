# !/usr/bin/python
# -*- coding:utf-8 -*-
from __future__ import print_function
from kazoo.client import KazooClient
import time
import uuid
import threading
import logging

logging.basicConfig()
my_id = uuid.uuid4()


CRAWLER_START_PATH = "/crawler/start"
START_FLAG = "start"
zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()


def leader_callback():
    print ("[Master] I am the leader {}".format(str(my_id)))
    if not zk.exists(CRAWLER_START_PATH):
        zk.create(path=CRAWLER_START_PATH, value=START_FLAG, ephemeral=True)
        print("[Master] {} triggered start flag".format(str(my_id)))
    while True:
        print("[Master] {} is working! ".format(str(my_id)))
        time.sleep(5)


def elect():
    election = zk.Election("/electionpath")
    election.run(leader_callback)
    zk.stop()


@zk.DataWatch(CRAWLER_START_PATH)
def watch_crawler_start(data, stat):
    if data == START_FLAG:
        print("[Slave]crawling is starting, %s, %s" % (data, stat.version))
    else:
        time.sleep(1)


if __name__ == '__main__':
    election_thread = threading.Thread(target=elect)
    election_thread.start()
    election_thread.join()
