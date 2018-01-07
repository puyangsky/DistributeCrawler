# !/usr/bin/python
# -*- coding:utf-8 -*-
from __future__ import print_function
from kazoo.client import KazooClient
import time
import uuid
import threading
import logging

logging.basicConfig()


class ZkThread(threading.Thread):
    def __init__(self, host, port):
        super(ZkThread, self).__init__()
        self.CRAWLER_START_PATH = "/crawler/start"
        self.START_FLAG = "start"
        self.election_path = "/election_path"
        self.my_id = uuid.uuid4()
        self.zk = KazooClient(hosts=":".join((host, port)))
        self.zk.start()
        self.watch()

    def load_seed(self):
        """
        load seed by master
        to be override by user
        """
        pass

    def fetch(self):
        """
        fetch data by slave
        to be override by user
        """
        pass

    def leader_callback(self):
        print("[Master] I am the leader {}".format(str(self.my_id)))
        self.load_seed()
        if not self.zk.exists(self.CRAWLER_START_PATH):
            self.zk.create(path=self.CRAWLER_START_PATH, value=self.START_FLAG, ephemeral=True)
            print("[Master] {} triggered start flag".format(str(self.my_id)))

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
    election_thread = ZkThread("localhost", 2181)
    election_thread.start()
    election_thread.join()
