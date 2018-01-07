#!/usr/bin/env python
# encoding: utf-8

"""
@author: puyangsky
@file: log_util.py
@time: 2018/1/7 下午11:51
"""
import logging
import os


class MyLog:
    def __init__(self):
        parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        log_file_path = os.path.join(parent_dir, 'log/crawler.log')
        print(log_file_path)
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename=log_file_path,
                            filemode='w')
        # remove useless log in requests
        logging.getLogger("requests").setLevel(logging.ERROR)
        logging.getLogger("urllib3").setLevel(logging.WARNING)

    @staticmethod
    def info(msg, *args, **kwargs):
        logging.info(msg, *args, **kwargs)

    @staticmethod
    def warning(msg, *args, **kwargs):
        logging.warning(msg, *args, **kwargs)

    @staticmethod
    def debug(msg, *args, **kwargs):
        logging.debug(msg, *args, **kwargs)

    @staticmethod
    def error(msg, *args, **kwargs):
        logging.error(msg, *args, **kwargs)
