# !/usr/bin/python
# -*- coding:utf-8 -*-

import os
import shutil


def merge(src_dir_name, target_filename):
    """
    合并src_dir目录下的所有文件到target_file中
    :param src_dir_name:
    :param target_filename:
    :return:
    """
    files = os.listdir(src_dir_name)
    for file_ in files:
        if "error" in file_:
            continue
        path = os.path.join(src_dir_name, file_)
        if os.path.isfile(path):
            print("copy %s to %s" % (path, target_filename))
            shutil.copyfileobj(open(path, "r"), open(target_filename, "a+"))
    print("done!")


if __name__ == '__main__':
    merge("./data/", "./data/merge_file.dat")
