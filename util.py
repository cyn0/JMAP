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
        start_time_1 = timeit.default_timer()
        JMAPPER.insert_json(item)
        elapsed_1 = timeit.default_timer() - start_time_1

        start_time_2 = timeit.default_timer()
        json_util.insert_json(item)
        elapsed_2 = timeit.default_timer() - start_time_2

        logger.info("Time taken to Insert: JMapper: {0}, Inbuilt json: {1}".format(elapsed_1,elapsed_2))

def read_json():
    pass

def update_json():
    start_time_1 = timeit.default_timer()
    JMAPPER.update("fabl", "updated_fablkey")
    elapsed_1 = timeit.default_timer() - start_time_1

    start_time_2 = timeit.default_timer()
    json_util.insert_json(item)
    elapsed_2 = timeit.default_timer() - start_time_2

    logger.info("Time taken to Insert: JMapper: {0}, Inbuilt json: {1}".format(elapsed_1, elapsed_2))

def run_sample_tests():
    insert_sample_json()
