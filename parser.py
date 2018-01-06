# !/usr/bin/python
# -*- coding:utf-8 -*-
import time
id_map = {}
visited_id = set()


def parse():
    with open("xmly_sum.txt", "r") as f:
        print time.time()
        total_count = 0
        for line in f.readlines():
            if line == "":
                continue
            else:
                line = line.strip('\n')
                # print(line)
                items = line.split("/")
                key = items[-1]
                id_map[key] = line
                total_count += 1
        print("total: %d, id : %d" % (total_count, len(id_map)))

        print time.time()
        with open("data/xmly.dat", "r") as f1:
            while True:
                line = f1.readline()
                if line == "": break
                line = line.strip('\n')
                items = line.split("\t")
                if len(items) != 2: continue
                key = items[0]
                value = items[1]
                id_ = key.split("/")[-1].split(".")[0]
                # print(id_)
                if id_ in visited_id: continue
                url = id_map.get(id_, "")
                if url == "": continue
                with open("data/xmly_new.dat", "a+") as f2:
                    f2.write("%s\t%s\n" % (url, value))
                visited_id.add(id_)
        print time.time()
        print "write %d into file" % len(visited_id)


def check():
    with open("data/xmly_new.dat", "r") as f1:
        url_set = set()
        index = 0
        while True:
            line = f1.readline()
            if line == "": break
            key = line.split("\t")[0]
            url_set.add(key)
            index += 1
            if index == 140709 or index == 140710:
                print(line)
        print("total size: %d, set size: %d" % (index, len(url_set)))


def filter_already_handled_url():
    id_url_map = dict()
    id_set = set()
    with open("xmly_sum.txt", "r") as f:
        for line in f.readlines():
            line = line.strip("\n")
            id_url_map[line.split("/")[-1]] = line
        print("id_url_map size: %d" % len(id_url_map))
        with open("./data/merge_file.dat", "r") as f1:
            while True:
                line = f1.readline().strip("\n")
                if line == "": break
                items = line.split("\t")
                if len(items) != 2: continue
                key = items[0]
                id_ = key.split("/")[-1].split(".")[0]
                id_set.add(id_)
        print("id set size: %d" % len(id_set))
        with open("filterd_xmly_sum.dat", "a+") as f2:
            for k, v in id_url_map.items():
                if k not in id_set:
                    f2.write("%s\n" % v)


if __name__ == '__main__':
    # parse()
    # check()
    filter_already_handled_url()