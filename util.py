import json
from core.jmapper import JMAPPER
from core.json_util import JsonUtil
import timeit
import random
import logging
from multiprocessing.dummy import Pool
import os

logger = logging.getLogger(__name__)

JMAPPER = JMAPPER()
json_util = JsonUtil()
#BaseDB = BaseDB()

def insert_sample_json():
    json_data = open('simple', 'r+')
    jdata = json.loads(json_data.read().decode("utf-8"))

    for item in jdata:
        JMAPPER.insert_json(item)
        json_util.insert_json(item)

json_data = open('sample.json', 'r+')
jdata = json.loads(json_data.read().decode("utf-8"))
jmapper_key = []

def preprocess():
    lookup_rows = JMAPPER.read_lookup(None)
    print "In Preprocess ", lookup_rows
    for row in lookup_rows:
        key = str(row[1])
        jmapper_key.append(key)

def update_json(json_obj):
    #print "In update json"
    #print "jmapper_key %s", jmapper_key
    #curr_json_obj = random.choice(json_obj)
    #print "curr_json_object", json_obj
    #print "Json_object", curr_json_obj

    key = random.choice(json_obj.keys())

    old_cur=None
    cur = json_obj[key]
    update_path = key
    while isinstance(cur, dict):
        key = random.choice(cur.keys())
        update_path = update_path + "." + key
        old_cur=cur
        cur = cur[key]
    print "update Path: ", update_path

    condition_key = random.choice(json_obj.keys())
    condition_value =  json_obj[condition_key]

    while isinstance(condition_value, dict):
        condition_key = random.choice(json_obj.keys())
        condition_value = json_obj[condition_key]


    print "condition path", condition_key
    print "condition value", condition_value
    if old_cur is None:
        json_obj[key]="sabaskar_key"
    else:
        old_cur[key]="sabaskar_key"
        #key = "r.ra.rab.raaa"
    JMAPPER.update_json(update_path, "sabaskar_key", condition_key, str(condition_value), "jMapper_update")
    json_util.update_json(update_path.replace(".", ", "), "updated_key" + key, "postgress_update")


def read_json(json_object):
    # print "Reading key:{0} Value:".format(json_object)
    key = random.choice(json_object.keys())
    val = json_object[key]
    print "*"*100
    print "Reading key:{0} Value:{1}".format(key, val)
    print "*" * 100
    print type(val)
    if isinstance(val, unicode):
        print "Executing"
        print len(JMAPPER.get_json(condition=key, condition_value=str(val)))
        print "#"*200
        print len(json_util.get_json(key,val))

def random_read():
    #optimise?!
    import json
    json_dir_name = "./JsonData"
    logger.info("Loading data from '{}'".format(json_dir_name))
    dir = os.path.expanduser(json_dir_name)

    whole_data = []
    for root, dirs, files in os.walk(dir):
        for f in files:
            fname = os.path.join(root, f)
            if not fname.endswith(".json"):
                continue
            with open(fname) as js:
                data = json.loads(js.read().decode("utf-8"))

            whole_data = whole_data + data

    # print json.dumps(whole_data)
    # print "#"*100

    key = random.choice(whole_data)
    read_json(key)
    # pool = Pool(30)
    # arg = reservoir_random_sample(whole_data, 30)
    # pool.map(read_json, arg)
    # pool.close()
    # pool.join()

def run_sample_tests():
    #insert_sample_json()
    insert_multiple_json("./JsonData")
    #preprocess()
    #update_json("q")
    update_concurrency_test()
    # print JMAPPER.get_json(condition="id", condition_value="xiq2-ahjv")
    # random_read()

def insert_multiple_json(json_dir_name):
     logger.info("Loading data from '{}'".format(json_dir_name))
     dir = os.path.expanduser(json_dir_name)
     for root, dirs, files in os.walk(dir):
         for f in files:
             fname = os.path.join(root, f)
             if not fname.endswith(".json"):
                 continue
             with open(fname) as js:
                 data = json.loads(js.read().decode("utf-8"))

             for item in data:
                 JMAPPER.insert_json(item)
                 json_util.insert_json(item)

def update_concurrency_test():
    preprocess()
    pool = Pool(processes=1)

    json_dir_name = "./JsonData"
    logger.info("Loading data from '{}'".format(json_dir_name))
    dir = os.path.expanduser(json_dir_name)

    whole_data = []
    for root, dirs, files in os.walk(dir):
        for f in files:
            fname = os.path.join(root, f)
            if not fname.endswith(".json"):
                continue
            with open(fname) as js:
                data = json.loads(js.read().decode("utf-8"))

            whole_data = whole_data + data
    arg = reservoir_random_sample(whole_data, 30)
    pool.map(update_json, arg)
    print "$"*100
    pool.close()
    pool.join()

    # key = random.choice(whole_data)
    # update_json(key)

def reservoir_random_sample(input, N):
    sample = []
    for i, line in enumerate(input):
        if i < N:
            sample.append(line)
        elif i >= N and random.random() < N / float(i + 1):
            replace = random.randint(0, len(sample) - 1)
            sample[replace] = line
    return sample