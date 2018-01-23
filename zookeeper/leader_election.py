# !/usr/bin/python
# -*- coding:utf-8 -*-
from kazoo.client import KazooClient
import time
import uuid
import threading
from utils import log_util
from utils import config_util
import redis
from concurrent.futures import ThreadPoolExecutor

LOG = log_util.MyLog()


class ZkThread(threading.Thread):
    def __init__(self, crawler_name):
        super(ZkThread, self).__init__()
        self.crawler_name = crawler_name
        self.crawler_path = "/crawler/" + self.crawler_name
        self.master_path = self.crawler_path + "/master"
        self.node_path = self.crawler_path + "/node"
        self.election_path = self.crawler_path + "/election"
        self.start_flag = "start".encode('utf-8')
        self.master_flag = False
        self.crawler_id = str(uuid.uuid4())
        self.thread_pool = ThreadPoolExecutor(max_workers=5)
        self.zk = KazooClient(hosts=config_util.zk_host)
        self.redis_pool = redis.ConnectionPool(host=config_util.redis_host,
                                               port=config_util.redis_port,
                                               db=0)
        self.register_node()
        self.watch()

    def register_node(self):
        self.zk.start()
        self.zk.ensure_path(self.node_path)
        self.zk.create(path=self.node_path + "/" + self.crawler_id,
                       ephemeral=True)
        children = self.zk.get_children(self.node_path)
        print("Current children cnt: %d" % len(children))

    def load_seed(self):
        """
        load seed by master
        to be override by user
        """
        print("load begin")
        while True:
            time.sleep(7)

    def fetch(self):
        """
        fetch data by slave
        to be override by user
        """
        print("fetch begin")
        while True:
            time.sleep(7)

    def leader_callback(self):
        print("[Master] I am the leader {}".format(self.crawler_id))
        self.master_flag = True
        if not self.zk.exists(self.master_path):
            self.zk.create(path=self.master_path,
                           value=self.start_flag,
                           ephemeral=True)
            print("[Master] {} triggered start flag".format(self.crawler_id))
        self.load_seed()

    def elect(self):
        print("Begin electing")
        election = self.zk.Election(self.election_path, identifier=self.crawler_id)
        election.run(self.leader_callback)

    def run(self):
        self.elect()

    def watch(self):
        @self.zk.DataWatch(self.master_path)
        def watch_crawler_start(data, stat):
            if data == self.start_flag:
                if not self.master_flag:
                    print("[Slave] crawling is starting, %s" % data)
                    self.thread_pool.submit(fn=self.fetch)

        @self.zk.ChildrenWatch(self.node_path)
        def watch_node(children):
            print("watch children, now children cnt: %d" % len(children))


if __name__ == '__main__':
    election_thread = ZkThread("test_crawler")
    election_thread.start()
    election_thread.join()
