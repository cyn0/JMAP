from jmapper import JMAPPER
from json_util import JsonUtil

def build_schema():
    jmapper = JMAPPER()
    jmapper.create_lookup_table()
    jmapper.create_string_table()

    json_util = JsonUtil()
    json_util.create_json_table()






