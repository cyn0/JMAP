import json
import logging
from core.db import select_all_lookup
from core.db import insert_lookup_table
from core.schema_builder import build_schema

json_data = open('sample.json', 'r+')
jdata = json.loads(json_data.read().decode("utf-8"))

def initialise_service():
    logging.basicConfig(format='[%(asctime)s %(levelname)s]: %(message)s [%(filename)s:%(lineno)s - %(funcName)s]',
                        level=logging.DEBUG)

    logging.info("Initialising...")
    build_schema()

def start_service():
    pass

def padding(prefix):
    id = str(prefix)
    if prefix < 10:
        id = str(0) + id
    return id

def id_generator(dict_var, prefix):
    for k, v in dict_var.iteritems():
       # print k, v, count
       print "{0} ---> {1}".format(k,  prefix + 1)
       prefix = prefix + 1
       id = padding(prefix)
       insert_lookup_table(({"id":id, "field": k}))
       if isinstance(v, dict):
            id_generator(v, (prefix * 100) + 1)
       else:
           pass

def main():
    initialise_service()
    start_service()
    id_generator(jdata, 0)
    row = select_all_lookup()
    print row

if __name__ == '__main__':
    main()

