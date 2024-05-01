
from audioop import reverse
import enum
from platform import node
from telnetlib import Telnet
import numpy as np
# import matplotlib.pyplot as plt
from scipy import special
import sys
import logging
import os
import string
import random
from zipf_generator import *



logging.basicConfig(level=logging.NOTSET)

# unused
# def save_plot(raw_data, a, write_dir):
#     plt.clf()
#     count, bins, ignored = plt.hist(raw_data[raw_data<50], 50)
#     x = np.arange(1., 50.)
#     y = (x**(-a) / special.zetac(a)) 
    
#     plt.plot(x, y/max(y), linewidth=2, color='r')
#     plt.title("num_row : {}".format(len(raw_data)))
#     plt.savefig(write_dir + "{}_{}.jpg".format(str(len(raw_data)),str(a)))
    
def random_str(length):
    rest_length = length
    rand_strs = []
    while rest_length > 0:
        l = min(32, rest_length)
        rand_strs.append(''.join(random.sample(string.ascii_letters + string.digits, l)))
        rest_length -= l
    ret_str = ''.join(rand_strs)
    return ret_str

def write_data(raw_data, file_name):
    with open (file_name, 'w') as f:
        for i, x in enumerate(raw_data):
            rand_str = random_str(256)
            f.write("%s,%s"%(str(x), rand_str))
            f.write('\n')


def write_skew(raw_data, a, threshold, write_dir):
    skew_name = "t{}_{}".format(str(len(raw_data)),a)
    threshold_count = threshold * len(raw_data)
    map = dict()
    res = dict()
    for x in raw_data:
        map[x] = map.get(x,0) + 1

    for key in map.keys():
        if map[key] > threshold_count:
            res[key] = map[key]

    rr = sorted(res.items(), key = lambda kv:kv[1], reverse=True)
    with open (write_dir + skew_name + ".skew", 'w') as f:
        for skew_data in rr:
            f.write("%s,%s"%(str(skew_data[0]), str(skew_data[1])))
            f.write('\n')
    
    return res


# unused 
def insert_single_skew(raw_data1, raw_data2, a1, a2):
    nums1 = len(raw_data1) * a1
    nums2 = len(raw_data2) * a2

    for item in enumerate(raw_data1):
        if item[0] >= nums1: break
        raw_data1[item[0]] = 1


    for item in enumerate(raw_data2):
        if item[0] >= nums2: break
        raw_data2[item[0]] = 1
# unused 
def intersect_process(raw_data1, raw_data2, threshold, intersect_rate):
    count1 = dict()
    skew1 = dict()
    threshold_count1 = threshold * len(raw_data1)
    for x in raw_data1:
        count1[x] = count1.get(x,0) + 1
    for key in count1.keys():
        if count1[key] > threshold_count1:
            skew1[key] = count1[key]
    res1 = sorted(skew1.items(), key = lambda kv:kv[1], reverse=True)

    count2 = dict()
    skew2 = dict()
    threshold_count2 = threshold * len(raw_data2)
    for x in raw_data2:
        count2[x] = count2.get(x,0) + 1
    for key in count2.keys():
        if count2[key] > threshold_count2:
            skew2[key] = count2[key]
    res2 = sorted(skew2.items(), key = lambda kv:kv[1], reverse=False)

    # skew_nums = min(11, min(len(res1), len(res2)))
    # res1 = res1[0:skew_nums]
    # res2 = res2[0:skew_nums]
    # intersect_nums = skew_nums * intersect_rate
    
    replace_elems = dict()
    replace_elems[res2[0][0]] = res1[0][0]
    # for item in enumerate(res1):
    #     index = item[0]
    #     if (index >= intersect_nums): break
    #     replace_elems[res2[index][0]] = res1[index][0]
    
    # replace
    for item in enumerate(raw_data2):
        i = item[0]
        val = item[1]
        if (replace_elems.get(val, None) == None): continue
        raw_data2[i] = replace_elems.get(val)

def parse_config():
    config = {}
    with open("../../config/join-compare.ini") as file:
        for item in file.readlines():
            if item.find('=') != -1:
                line = item.replace('\n', '')
                strs = line.split(sep = "=")
                config[strs[0]] = strs[1]
    return config

def split_list_n_list(origin_list, n):
    size = len(origin_list)
    half_size = size / 2
    split_size = []
    for i in range(n):
        if i == n-1:
            split_size.append(size)
            break
        length = random.randint(0, min(half_size, size))
        size -= length
        split_size.append(length)

    idx = 0
    for i, length in enumerate(split_size):
        start = idx
        end = idx + length
        idx += length
        logging.info("node{}数据量 : {}".format(i, length))
        yield origin_list[start:end]

    # if len(origin_list) % n == 0:
    #     cnt = len(origin_list) // n
    # else:
    #     cnt = len(origin_list) // n + 1
 
    # for i in range(0, n):
    #     yield origin_list[i*cnt:(i+1)*cnt]

if __name__ == "__main__":
    config = parse_config()
    node_nums = int(config['node_nums'])
    small_skew_degree = float(config['small_skew_degree'])
    big_skew_degree = float(config['big_skew_degree'])
    small_table_size = int(config['small_table_size'])
    big_table_size = int(config['big_table_size'])
    frequency_threshold = float(config['frequency_threshold'])

    write_dir = '../{}_{}_{}_{}_{}/'.format(str(node_nums), str(small_table_size), str(small_skew_degree), str(big_table_size), str(big_skew_degree))
    if not os.path.exists(write_dir):
        os.mkdir(write_dir)

    bzg_factory_instance = bzg_factory()
    bzg_1 = bzg_factory_instance.create(small_skew_degree, small_table_size, small_table_size)
    bzg_2 = bzg_factory_instance.create(big_skew_degree, big_table_size, big_table_size)
    small_table = bzg_1.generate()
    big_table = bzg_2.generate()
    for i in range(10):
        random.shuffle(small_table)
        random.shuffle(big_table)

    small_skew = write_skew(small_table, small_skew_degree, frequency_threshold, write_dir)
    big_skew = write_skew(big_table, big_skew_degree, frequency_threshold, write_dir)

    small_after_split = split_list_n_list(small_table, node_nums)
    big_after_split = split_list_n_list(big_table, node_nums)

    logging.info("writing small_table, num_row = {}...".format(small_table_size))
    for node_id, list in enumerate(small_after_split):
        node_dir = write_dir + str(node_id)
        if not os.path.exists(node_dir):
            os.mkdir(node_dir)
        table_name = node_dir + '/small'
        write_data(list, table_name)

        part_skew = {}
        for value in list :
            if small_skew.get(value) != None: 
                part_skew[value] = part_skew.get(value, 0) + 1
        with open (node_dir + '/small.skew', 'w') as f:
            for item in part_skew.items():
                key = item[0]
                val = item[1]
                f.write("%s,%s"%(str(key), str(val)))
                f.write('\n')
        

    logging.info("writing big_table, num_row = {}...".format(big_table_size))
    for node_id, list in enumerate(big_after_split):
        node_dir = write_dir + str(node_id)
        table_name = node_dir + '/big'
        write_data(list, table_name)

        part_skew = {}
        for value in list :
            if big_skew.get(value) != None: 
                part_skew[value] = part_skew.get(value, 0) + 1
        with open (node_dir + '/big.skew', 'w') as f:
            for item in part_skew.items():
                key = item[0]
                val = item[1]
                f.write("%s,%s"%(str(key), str(val)))
                f.write('\n')

    

            
    
