from jmapper import JMAPPER
from json_util import JsonUtil
jmapper = JMAPPER()
json_util = JsonUtil()

def build_schema():
    create_tables()
    create_indices()

def create_tables():
    jmapper.create_lookup_table()
    jmapper.create_string_table()

    json_util.create_json_table()

def create_indices():
    jmapper.create_index()
    json_util.create_index()

def delete_tables():
    jmapper = JMAPPER()
    jmapper.drop_table()

    json_util = JsonUtil()
    json_util.drop_table()





