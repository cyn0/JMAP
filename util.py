import json
from core.jmapper import JMAPPER
from core.json_util import JsonUtil
import timeit
import random
import logging
from multiprocessing.dummy import Pool

logger = logging.getLogger(__name__)

JMAPPER = JMAPPER()
json_util = JsonUtil()

def insert_sample_json():
    json_data = open('simple', 'r+')
    jdata = json.loads(json_data.read().decode("utf-8"))

    for item in jdata:
        JMAPPER.insert_json(item)
        json_util.insert_json(item)

def read_json():
    pass


json_data = open('sample.json', 'r+')
jdata = json.loads(json_data.read().decode("utf-8"))

def update_json(p):
    #key = random.choice(jdata[0].keys())
    JMAPPER.update_json("r.ra.rab.raba", "sabaskar_updated_key")
    json_util.update_json("r, ra, rab, raba", "updated_key")

def run_sample_tests():
    insert_sample_json()
    update_json("q")
    #update_concurrency_test()

def update_concurrency_test():
    pool = Pool(processes=200)
    arg=[]
    for i in range(202):
        arg.append("q")
    pool.map(update_json, arg)
    print "$"*100
    pool.close()
    pool.join()
