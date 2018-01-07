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


class ZkThread(threading.Thread):
    def __init__(self):
        super(ZkThread, self).__init__()
        self.CRAWLER_START_PATH = "/crawler/start"
        self.START_FLAG = "start"
        self.election_path = "/election_path"
        self.zk = KazooClient(hosts='127.0.0.1:2181')
        self.zk.start()
        self.watch()

    def leader_callback(self):
        print("[Master] I am the leader {}".format(str(my_id)))
        if not self.zk.exists(self.CRAWLER_START_PATH):
            self.zk.create(path=self.CRAWLER_START_PATH, value=self.START_FLAG, ephemeral=True)
            print("[Master] {} triggered start flag".format(str(my_id)))
        while True:
            print("[Master] {} is working! ".format(str(my_id)))
            time.sleep(5)

    def run(self):
        election = self.zk.Election(self.election_path)
        election.run(self.leader_callback)

    def watch(self):
        @self.zk.DataWatch(self.CRAWLER_START_PATH)
        def watch_crawler_start(data, stat):
            if data == self.START_FLAG:
                print("[Slave]crawling is starting, %s, %s" % (data, stat.version))
            else:
                time.sleep(1)


if __name__ == '__main__':
    election_thread = ZkThread()
    election_thread.start()
    election_thread.join()
