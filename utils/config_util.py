#!/usr/bin/env python
# encoding: utf-8

from __future__ import print_function

"""
@author: puyangsky
@file: config_util.py
@time: 2018/1/6 下午5:08
"""
import ConfigParser
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
config_path = os.path.join(parent_dir, 'config.cfg')
if not os.path.isfile(config_path):
    raise BaseException('Config file not exists')

config = ConfigParser.ConfigParser()
config.read(config_path)

redis_host = config.get('redis', 'redis_host')
zk_host = config.get('zookeeper', 'zookeeper_host')
