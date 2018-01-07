# !/usr/bin/python
# -*- coding:utf-8 -*-
import json
from utils import log_util
import time
from threading import *

import redis
import requests

import status

handled_count = 0
logger = log_util.MyLog


class Crawler(object):
    def __init__(self, host='localhost', port=6379, thread_num=10, bucket_size=200, result_file="data/result-%s"
                                                                                                ".dat"):
        pool = redis.ConnectionPool(host=host, port=port, db=0)
        self.redis_conn = redis.Redis(connection_pool=pool)
        self.file_path = "xmly_sum.txt"
        self.queue_key = "XMLY_QUEUE"
        self.status_key = "XMLY_STATUS"
        self.queue_size = "XMLU_QUEUE_SIZE"
        self.thread_num = thread_num
        self.bucket_size = bucket_size
        self.lock = Lock()
        self.error_lock = Lock()
        self.result_file = result_file

    def load_seed(self):
        if self.redis_conn.get(self.status_key) == status.READY and self.redis_conn.exists(self.queue_key):
            logger.warning("queue already exists...")
        else:
            logger.info("Begin push urls into redis")
            url_set = set()
            with open(self.file_path, 'r') as f:
                for line in f.readlines():
                    line = line.strip('\n').strip('/')
                    url_set.add(line)
            for url in url_set:
                self.redis_conn.lpush(self.queue_key, url)
            self.redis_conn.set(self.status_key, status.READY)
            self.redis_conn.set(self.queue_size, len(url_set))
            logger.info("Push %d urls into redis: %s" % (len(url_set), self.queue_key))

    def remove_queue(self):
        self.redis_conn.delete(self.queue_key)
        self.redis_conn.delete(self.status_key)

    def crawl(self):
        self.load_seed()
        threads = []
        for index in range(self.thread_num):
            thread_id = "Thread-%d" % index
            crawler = CrawlerThread(thread_id, self.redis_conn, self.queue_key,
                                    self.bucket_size, self.lock, self.error_lock, self.result_file)
            crawler.setDaemon(True)
            crawler.start()
            logger.info("Thread: %s starts" % thread_id)
            threads.append(crawler)
        for thread in threads:
            thread.join()


class CrawlerThread(Thread):
    def __init__(self, thread_id, redis_conn, key, bucket_size, lock, error_lock, result_file):
        Thread.__init__(self)
        self.thread_id = thread_id
        self.redis_conn = redis_conn
        self.key = key
        self.bucket_size = bucket_size
        self.bucket = dict()
        self.lock = lock
        self.error_lock = error_lock
        self.result_file = result_file % self.thread_id
        self.url_pattern = "http://www.ximalaya.com/tracks/%s.json"
        self.header = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu "
                          "Chromium/62.0.3202.89 Chrome/62.0.3202.89 Safari/537.36",
        }
        self.last_update_time = time.time()
        self.timeout_url_file = "data/error.dat"

    def run(self):
        while True:
            # TODO(puyangsky): use `lrange index index+100` to batch fetch data instead of fetching one by one
            origin_url = self.redis_conn.rpop(self.key)
            if origin_url is None:
                logger.warning("There is no urls left in redis, exit thread...")
                break
            else:
                url = self.url_pattern % origin_url.split('/')[-1]
                try:
                    res = self.execute(url, origin_url)
                except Exception as e:
                    logger.error(e.message)
                    res = False
                if not res:
                    continue

    def execute(self, url, origin_url):
        try:
            req = requests.get(url, timeout=20, headers=self.header)
        except requests.exceptions.Timeout:
            logger.error("timeout, %s" % url)
            self.handle_error(origin_url)
            return False
        if req.status_code != 200:
            logger.error("bad request: %s" % url)
            self.handle_error(origin_url)
            return False
        resp = req.content
        if len(resp) == 0:
            logger.error("bad request: %s" % url)
            self.handle_error(origin_url)
            return False

        if len(self.bucket) < self.bucket_size:
            self.bucket[origin_url] = resp
        else:
            with open(self.result_file, "a+") as f:
                for key, value in self.bucket.items():
                    js = json.loads(value)
                    if "res" in js and js["res"] is False:
                        # logger.error("%s drop useless url: %s, res: %s" % (self.thread_id, key, value))
                        continue
                    js = self.unicode_to_utf8(js)
                    f.write("%s\t%s\n" % (key, json.dumps(js, ensure_ascii=False)))
            now = time.time()
            global handled_count
            handled_count += len(self.bucket)
            logger.info("%s flushed %d into file %s, cost: %.2f s, total handled count: %d" %
                         (self.thread_id, len(self.bucket), self.result_file, now - self.last_update_time,
                          handled_count))
            self.bucket.clear()
            self.last_update_time = now
        return True

    def handle_error(self, url):
        self.error_lock.acquire()
        with open(self.timeout_url_file, "a+") as f:
            f.write("%s\n" % url)
        self.error_lock.release()

    @staticmethod
    def unicode_to_utf8(self, js):
        if type(js) == dict:
            norm = {}
            for key in js:
                norm[self.unicode_to_utf8(key)] = self.unicode_to_utf8(js[key])
            return norm
        if type(js) == list:
            norm = []
            for item in js:
                norm.append(self.unicode_to_utf8(item))
            return norm
        if type(js) == unicode:
            return js.encode("utf-8", "ignore")
        return js


if __name__ == '__main__':
    c = Crawler(host='localhost', port=6379)
    # c.remove_queue()
    c.crawl()
