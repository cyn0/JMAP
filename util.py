import json
from core.jmapper import JMAPPER
from core.json_util import JsonUtil
import timeit
import random
import logging
import threading
from multiprocessing.dummy import Pool
import os

logger = logging.getLogger(__name__)

JMAPPER = JMAPPER()
json_util = JsonUtil()


json_data = open('sample.json', 'r+')
jdata = json.loads(json_data.read().decode("utf-8"))

json_dir_name = "./JsonData"


def run_sample_tests(num_process=1):
    insert_multiple_json(num_process)
    update_concurrency_test(num_process)
    random_read(num_process)

#####################################################################################
#################################INSERTS#############################################

def insert_jmapper(json_obj):
    JMAPPER.insert_json(json_obj)

def insert_json_default(json_obj):
    json_util.insert_json(json_obj)

def insert_multiple_json(num_process=1):
     logger.info("About to run insert json test case.")
     logger.info("Loading data from '{}'".format(json_dir_name))
     dir = os.path.expanduser(json_dir_name)
     for root, dirs, files in os.walk(dir):
         for f in files:
             fname = os.path.join(root, f)
             if not fname.endswith(".json"):
                 continue
             with open(fname) as js:
                 data = json.loads(js.read().decode("utf-8"))


             logger.info("Inserting data from file {0}. Number of json objects {1}".format(fname, len(data)))
             logger.info("Starting to insert using JMAPPER")
             pool = Pool(processes=num_process)
             pool.map(insert_jmapper, data)
             pool.close()
             pool.join()

             logger.info("Starting to insert using Postgres json")
             pool = Pool(processes=num_process)
             pool.map(insert_json_default, data)
             pool.close()
             pool.join()

def insert_sample_json():
    json_data = open('simple', 'r+')
    jdata = json.loads(json_data.read().decode("utf-8"))

    for item in jdata:
        JMAPPER.insert_json(item)
        json_util.insert_json(item)

#####################################################################################


#####################################################################################
################################# READS #############################################
def read_json_jmapper(json_object):
    key = random.choice(json_object.keys())
    val = json_object[key]

    # randomly reselect until its a field
    while not isinstance(val, unicode):
        key = random.choice(json_object.keys())
        val = json_object[key]

    # print "Reading key {0} and value {1}".format(key, str(val))
    JMAPPER.get_json(condition=key, condition_value=str(val))

def read_json_default(json_object):
    key = random.choice(json_object.keys())
    val = json_object[key]

    # randomly reselect until its a field
    while not isinstance(val, unicode):
        key = random.choice(json_object.keys())
        val = json_object[key]

    # print "Reading key {0} and value {1}".format(key, str(val))
    json_util.get_json(key, val)

def random_read(num_process=1):
    #optimise?!
    logger.info("About to run random read of json test case.")
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

    arg=[]

    #random sample 1000 items
    # arg = reservoir_random_sample(whole_data, 100)

    #one json 1000 times
    random_obj = random.choice(whole_data)
    for i in range(1000):
        arg.append(random_obj)

    pool = Pool(processes=num_process)
    pool.map(read_json_jmapper, arg)
    pool.close()
    pool.join()

    pool = Pool(processes=num_process)
    pool.map(read_json_default, arg)
    pool.close()
    pool.join()

#####################################################################################



#####################################################################################
################################# UPDATE #############################################

def update_json(json_obj):
    key = random.choice(json_obj.keys())

    old_cur = None
    cur = json_obj[key]
    update_path = key
    while isinstance(cur, dict):
        key = random.choice(cur.keys())
        update_path = update_path + "." + key
        old_cur = cur
        cur = cur[key]

    condition_key = random.choice(json_obj.keys())
    condition_value = json_obj[condition_key]

    while isinstance(condition_value, dict):
        condition_key = random.choice(json_obj.keys())
        condition_value = json_obj[condition_key]

    new_value = "updated_value"

    if old_cur is None:
        json_obj[key] = new_value
    else:
        old_cur[key] = new_value

    try:
        JMAPPER.update_json(update_path, new_value, condition_key, str(condition_value), "update_jmapper")
        json_util.update_json(update_path.replace(".", ", "), new_value, "update_json")

    except Exception as err:
        logger.error("{0}".format(err))


def update_concurrency_test(num_process=1):
    logger.info("About to run Random update of json test case.")
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

    arg = []
    # arg = reservoir_random_sample(whole_data, 500)

    random_obj = random.choice(whole_data)
    for i in range(500):
        arg.append(random_obj)

    pool = Pool(processes=1)
    pool.map(update_json, arg)
    pool.close()
    pool.join()

#####################################################################################


def reservoir_random_sample(input, N):
    sample = []
    for i, line in enumerate(input):
        if i < N:
            sample.append(line)
        elif i >= N and random.random() < N / float(i + 1):
            replace = random.randint(0, len(sample) - 1)
            sample[replace] = line
    return sample