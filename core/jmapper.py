from base_db import BaseDB
import psycopg2
import logging

logger = logging.getLogger(__name__)

LOOKUP_TABLE = "lookup_table"
LOOKUP_ID = "lookup_id"
LOOKUP_FIELD = "lookup_fied"

DATA_TABLE = "data_table"
DATA_OBJECT_ID = "data_object_id"
DATA_LOOKUP_ID = "data_lookup_id"
DATA_VALUE = "data_value"

ID_LENGTH = 8

class JMAPPER(BaseDB):
    def __init__(self):
        super(JMAPPER, self).__init__()

    def _insert_json_db(self, jsonList):
        insert_statement_lookup = "INSERT INTO " + LOOKUP_TABLE + "(lookup_id, lookup_fied) VALUES(%s, %s);"
        insert_statement_data_with_id = "INSERT INTO " + DATA_TABLE + "(data_object_id, data_lookup_id, data_value) VALUES(%s, %s, %s);"
        insert_statement_data = "INSERT INTO " + DATA_TABLE + "(data_lookup_id, data_value) VALUES(%s, %s) RETURNING data_object_id;"

        connection = None
        data_object_id = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            for item in jsonList:
                #logger.info("Inserting {0}".format(item))
                if item["type"] == "lookup":
                    cursor.execute(insert_statement_lookup, (item["lookup_id"], item["lookup_field"]))
                else:
                    if data_object_id:
                        cursor.execute(insert_statement_data_with_id,
                                       (data_object_id, item["data_lookup_id"], item["data_value"]))
                    else:
                        cursor.execute(insert_statement_data, (item["data_lookup_id"], item["data_value"]))
                        data_object_id = cursor.fetchone()[0]

            connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error("Error during DB operation {0}".format(error))
            raise

        finally:
            if connection is not None:
                connection.close()

    def flattenJson(self, jsonObject, level, prefix, flattenedList):
        cur = 0
        for k, v in jsonObject.iteritems():
            cur = cur + 1
            cur_key_value = str(cur)
            if cur < 10:   cur_key_value = "0" + cur_key_value

            c_prefix = prefix + cur_key_value
            no_of_zeros = ID_LENGTH - len(c_prefix)

            field = int(c_prefix + "0" * no_of_zeros)
            flattenedList.append({"lookup_id": field, "lookup_field": k, "type": "lookup"})

            if isinstance(v, dict):
                self.flattenJson(v, level + 1, c_prefix, flattenedList)

            else:
                flattenedList.append({"data_lookup_id": field, "data_value": v, "type": "data"})

    def insert_json(self, jObject):
        flattenedList = []
        self.flattenJson(jObject, 0, "", flattenedList)
        self._insert_json_db(flattenedList)

    def update_json(self, key, value):
        select_statement = "select lookup_id from lookup_table where lookup_fied=%s"
        update_statement = "UPDATE data_table SET data_value = %s WHERE data_lookup_id=%s;"
        connection = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            logger.info("Updating {0} value as {1}".format(key, value))
            cursor.execute(select_statement, (key, ))
            lookup_id = cursor.fetchone()[0]
            cursor.execute(update_statement, (value, lookup_id))
            connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error("Error during DB operation {0}".format(error))
            raise

        finally:
            if connection is not None:
                connection.close()

    def create_lookup_table(self):
        logger.info("Creating lookup table if not exists")
        command = "CREATE TABLE IF NOT EXISTS " \
                  + LOOKUP_TABLE + " (" \
                  + LOOKUP_ID + " SERIAL PRIMARY KEY," \
                  + LOOKUP_FIELD + " CHARACTER(255) NOT NULL" \
                                   ")"

        self.execute(command=command)

    def create_string_table(self):
        logger.info("Creating lookup table if not exists")
        command = "CREATE TABLE IF NOT EXISTS " \
                  + DATA_TABLE + " (" \
                  + DATA_OBJECT_ID + " SERIAL," \
                  + DATA_LOOKUP_ID + " INTEGER NOT NULL REFERENCES " + LOOKUP_TABLE + "(" + LOOKUP_ID + ")," \
                  + DATA_VALUE + " CHARACTER(255) NOT NULL" \
                                 ")"
        self.execute(command=command)

    def create_index(self):
        logger.info("Creating indices on data and lookup tables")

        #b-tree index
        # create_index_statement_lookup = "CREATE UNIQUE INDEX " + LOOKUP_ID + \
        #                                 " ON " + LOOKUP_TABLE + \
        #                                 " (" + LOOKUP_ID + ");"
        # create_index_statement_data = "CREATE UNIQUE INDEX " + DATA_LOOKUP_ID + \
        #                               " ON " + DATA_TABLE + \
        #                               " (" + DATA_LOOKUP_ID + ");"

        #Hash index
        create_index_statement_lookup = "CREATE INDEX " + LOOKUP_ID +\
                                        " ON " + LOOKUP_TABLE + \
                                        " USING HASH" + \
                                        " (" + LOOKUP_ID + ");"
        create_index_statement_data = "CREATE INDEX " + DATA_LOOKUP_ID + \
                                      " ON " + DATA_TABLE + \
                                      " USING HASH" + \
                                      " (" + DATA_LOOKUP_ID + ");"

        self.execute(command=create_index_statement_lookup)
        self.execute(command=create_index_statement_data)

    def drop_table(self):
        logger.info("Dropping lookup_table")
        self.execute("DROP TABLE lookup_table CASCADE;")

        logger.info("Dropping data_table")
        self.execute("DROP TABLE data_table;")