import json

json_data = open('./JsonData/views.json', 'r+')
jdata_file = json.loads(json_data.read().decode("utf-8"))

def remove_array_from_json(jdata):
    for k in jdata.keys():
        if isinstance(jdata[k], list):
            jdata.pop(k, None)
        elif isinstance(jdata[k], dict):
            remove_array_from_json(jdata[k])

def remove_array_from_json_file():
    for item in jdata_file:
        remove_array_from_json(item)

    with open('./JsonData/test.json', 'w') as outfile:
        json.dump(jdata_file, outfile)

remove_array_from_json_file()