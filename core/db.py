import psycopg2
import jsoncfg
import logging
import json

logger = logging.getLogger(__name__)
config = jsoncfg.load_config('configs.cfg')

LOOKUP_TABLE = "lookup_table"
LOOKUP_ID = "lookup_id"
LOOKUP_FIELD = "lookup_fied"

DATA_TABLE = "data_table"
DATA_OBJECT_ID = "data_object_id"
DATA_LOOKUP_ID = "data_lookup_id"
DATA_VALUE = "data_value"

ID_LENGTH = 8

def get_connection():
    db_config = config['db']
    connection = psycopg2.connect(database=db_config.name(),
                                  user=db_config.user(),
                                  password=db_config.password(),
                                  host=db_config.host(),
                                  port=db_config.port_num())
    return connection

def execute(command, dict=None):
    connection = None
    try:
        connection = get_connection()
        cursor = connection.cursor()

        logger.info("About to execute command: {0}".format(command))
        if dict is not None:
            cursor.execute(command, dict)
        else:
            cursor.execute(command)

        cursor.close()
        connection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error("Error during DB operation {0}".format(error))
        raise

    finally:
        if connection is not None:
            connection.close()

def insert_lookup_table(valuedict):
    logger.info("Inserting into lookup table")
    command = "INSERT INTO " + LOOKUP_TABLE +"("+ LOOKUP_ID + ", " + LOOKUP_FIELD + ")" \
              " VALUES (%(id)s, %(field)s)"
    execute(command=command, dict=valuedict)

def select_all_lookup():
    logger.info("Selecting from lookup table")
    command = "SELECT * FROM " + LOOKUP_TABLE
    execute(command=command)

def _insert_json_db(jsonList):
    insert_statement_lookup = "INSERT INTO "+ LOOKUP_TABLE + "(lookup_id, lookup_fied) VALUES(%s, %s);"
    insert_statement_data = "INSERT INTO " + DATA_TABLE + "(data_lookup_id, data_value) VALUES(%s, %s);"

    connection = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        for item in jsonList:
            logger.info("Inserting {0}".format(item))
            if item["type"] == "lookup":
                cursor.execute(insert_statement_lookup, (item["lookup_id"], item["lookup_field"]))
            else:
                cursor.execute(insert_statement_data, (item["data_lookup_id"], item["data_value"]))

        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error("Error during DB operation {0}".format(error))
        raise

    finally:
        if connection is not None:
            connection.close()

def flattenJson(jsonObject, level, prefix, flattenedList):
    cur=0
    for k, v in jsonObject.iteritems():
        cur=cur+1
        cur_key_value = str(cur)
        if cur<10:   cur_key_value = "0" + cur_key_value

        c_prefix = prefix + cur_key_value
        no_of_zeros = ID_LENGTH  - len(c_prefix)

        field = int(c_prefix + "0"* no_of_zeros)
        flattenedList.append({"lookup_id":field, "lookup_field":k, "type":"lookup"})

        if isinstance(v, dict):
            flattenJson(v, level+1, c_prefix, flattenedList)

        else:
            flattenedList.append({"data_lookup_id":field, "data_value":v, "type":"data"})

def insert_json(jObject):
    flattenedList = []
    flattenJson(jObject, 0, "", flattenedList)
    _insert_json_db(flattenedList)

def create_lookup_table():
    logger.info("Creating lookup table if not exists")
    command = "CREATE TABLE IF NOT EXISTS " \
                + LOOKUP_TABLE + " (" \
                + LOOKUP_ID + " SERIAL PRIMARY KEY," \
                + LOOKUP_FIELD + " CHARACTER(255) NOT NULL" \
                ")"

    execute(command=command)

def create_string_table():
    logger.info("Creating lookup table if not exists")
    command = "CREATE TABLE IF NOT EXISTS " \
              + DATA_TABLE + " (" \
              + DATA_OBJECT_ID + " SERIAL," \
              + DATA_LOOKUP_ID + " INTEGER NOT NULL REFERENCES " + LOOKUP_TABLE + "(" + LOOKUP_ID + ")," \
              + DATA_VALUE + " CHARACTER(255) NOT NULL" \
                ")"
    execute(command=command)

