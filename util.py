import json
from core.db import insert_json

def insert_sample_json():
    json_data = open('sample.json', 'r+')
    jdata = json.loads(json_data.read().decode("utf-8"))
    for item in jdata:
        insert_json(item)
