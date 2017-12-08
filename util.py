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
#BaseDB = BaseDB()

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
jmapper_key = []

def preprocess():
    lookup_rows = JMAPPER.read_lookup(None)
    print "In Preprocess ", lookup_rows
    for row in lookup_rows:
        key = str(row[1])
        jmapper_key.append(key)

def update_json(p):
    print "In update json"
    print "jmapper_key %s", jmapper_key
    key = random.choice(jmapper_key)
    #key = "r.ra.rab.raaa"
    JMAPPER.update_json(key, "a2key", "a", "a2key", "jMapper_update")
    #json_util.update_json(key.replace(".", ", "), "updated_key" + key, "postgress_update")

def run_sample_tests():
    insert_sample_json()
    #update_json("q")
    update_concurrency_test()
    print JMAPPER.get_json(condition="b", condition_value="updated_keyr.ra.rab.raaa")

def update_concurrency_test():
    preprocess()
    pool = Pool(processes=100)
    arg=[]
    for i in range(202):
        arg.append("q")
    pool.map(update_json, arg)
    print "$"*100
    pool.close()
    pool.join()
