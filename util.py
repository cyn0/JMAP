import json
from core.jmapper import JMAPPER
from core.json_util import JsonUtil
import timeit
import logging

logger = logging.getLogger(__name__)

JMAPPER = JMAPPER()
json_util = JsonUtil()

def insert_sample_json():
    json_data = open('sample.json', 'r+')
    jdata = json.loads(json_data.read().decode("utf-8"))

    for item in jdata:
        JMAPPER.insert_json(item)
        json_util.insert_json(item)

def read_json():
    pass

def update_json():
    JMAPPER.update_json("q", "updated_qkey")
    json_util.update_json("q", "updated_qkey")

def run_sample_tests():
    insert_sample_json()
    update_json()
