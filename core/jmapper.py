from base_db import BaseDB
import psycopg2
import timeit
import logging

from jmapper_util import JMapperUtil
from jmapper_util import ID_LENGTH

logger = logging.getLogger(__name__)

LOOKUP_TABLE = "lookup_table"
LOOKUP_ID = "lookup_id"
LOOKUP_FIELD = "lookup_fied"
LOOKUP_FIELD_LEVEL = "lookup_fied_level"

DATA_TABLE = "data_table"
DATA_OBJECT_ID = "data_object_id"
DATA_LOOKUP_ID = "data_lookup_id"
DATA_VALUE = "data_value"

class JMAPPER(BaseDB):
    update_concurrency_time = []
    
    def __init__(self):
        super(JMAPPER, self).__init__()
        self.jmapper_util = JMapperUtil(self)

    def read_lookup(self, filter):
        statement = "SELECT * from " + LOOKUP_TABLE + ";"
        return self.execute(statement, returnsResult=True)

    def _insert_json_db(self, jsonList):
        insert_statement_lookup = "INSERT INTO " + LOOKUP_TABLE + "(lookup_id, lookup_fied,lookup_fied_level) VALUES(%s, %s, %s);"
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
                    cursor.execute(insert_statement_lookup, (item["lookup_id"], item["lookup_field"], item["lookup_fied_level"]))
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


    # def flattenJson(self, jsonObject, level, prefix, flattenedList):
    #     from jmapper_util import JMapperUtil
    #     self.jmapper_util = JMapperUtil()
    #
    #     cur = self.jmapper_util.get_prefix_current_int(prefix)
    #
    #     for k, v in jsonObject.iteritems():
    #         cur = cur + 1
    #
    #         self.jmapper_util.incr_prefix_current_int(prefix)
    #         cur_key_value = str(cur)
    #         if cur < 10:   cur_key_value = "0" + cur_key_value
    #
    #         c_prefix = prefix + cur_key_value
    #         no_of_zeros = ID_LENGTH - len(c_prefix)
    #
    #         field = int(c_prefix + "0" * no_of_zeros)
    #         flattenedList.append({"lookup_id": field, "lookup_field": k, "type": "lookup","lookup_fied_level": level})
    #
    #         if isinstance(v, dict):
    #             self.flattenJson(v, level + 1, c_prefix, flattenedList)
    #
    #         else:
    #             flattenedList.append({"data_lookup_id": field, "data_value": v, "type": "data"})

    def flattenJson(self, jsonObject, level, prefix, file_name_prefix, flattenedList):
        statement = "SELECT * from " + LOOKUP_TABLE + " WHERE lookup_fied=%s;"

        cur = self.jmapper_util.get_prefix_current_int(prefix)

        connection = self.get_connection()
        cursor = connection.cursor()

        for k, v in jsonObject.iteritems():
            cursor.execute(statement, (k, ))

            result = cursor.fetchone()

            c_prefix = ""
            lookup_id = ""

            if result is None:
                cur = cur + 1

                self.jmapper_util.incr_prefix_current_int(prefix)
                cur_key_value = str(cur)
                if cur < 10:   cur_key_value = "0" + cur_key_value

                c_prefix = prefix + cur_key_value
                no_of_zeros = ID_LENGTH - len(c_prefix)

                lookup_id = int(c_prefix + "0" * no_of_zeros)
                lookup_field =  self.jmapper_util.append_field_path(file_name_prefix, k)
                flattenedList.append({"lookup_id": lookup_id, "lookup_field": lookup_field, "type": "lookup","lookup_fied_level": level})
            else:
                lookup_id = result[0]
                lookup_field = result[1]
                level = result[2]
                c_prefix = str(lookup_id)[:(level + 1) * 2]

            if isinstance(v, dict):
                self.flattenJson(v, level + 1, c_prefix, lookup_field, flattenedList)

            else:
                flattenedList.append({"data_lookup_id": lookup_id, "data_value": v, "type": "data"})

        cursor.close()

    def insert_json(self, jObject):
        flattenedList = []
        self.flattenJson(jObject, 0, "", None, flattenedList)
        self._insert_json_db(flattenedList)
        self.preprocessing() 

    def update_json(self, keyPath, value):
        select_lookupid_statement = "SELECT " + LOOKUP_ID + ", " + LOOKUP_FIELD_LEVEL +" from " + LOOKUP_TABLE + " WHERE " + LOOKUP_FIELD + "=%s"
        update_statement = "UPDATE " + DATA_TABLE + " SET "+ DATA_VALUE +" = %s WHERE "+ DATA_LOOKUP_ID +" =%s;"
        connection = None
        try:
            #logger.info("Updating {0} value as {1}".format(keyPath, value))
            start_time_1 = timeit.default_timer()
            connection = self.get_connection()
            cursor = connection.cursor()

            fieldId = self.jmapper_util.get_field_id_from_memory(keyPath)
            #fieldId = None
            if fieldId is None:
                cursor.execute(select_lookupid_statement, (keyPath, ))
                row = cursor.fetchone()
                if row is None:
                    return
                fieldId = row[0]

            cursor.execute(update_statement, (value, fieldId))
            cursor.close()
            connection.commit()

            elapsed_1 = timeit.default_timer() - start_time_1
            logger.info("Updating {0} value as {1}. Time taken to Update: JMapper: {2}".format(keyPath, value, elapsed_1))
            self.update_concurrency_time.append(elapsed_1)
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
                  + LOOKUP_FIELD + " TEXT NOT NULL," \
                  + LOOKUP_FIELD_LEVEL + " SMALLINT" \
                                   ")"
        logger.info("Creating lookup table lookup table")
        self.execute(command=command)

    def create_string_table(self):
        logger.info("Creating lookup table if not exists")
        command = "CREATE TABLE IF NOT EXISTS " \
                  + DATA_TABLE + " (" \
                  + DATA_OBJECT_ID + " SERIAL," \
                  + DATA_LOOKUP_ID + " INTEGER NOT NULL REFERENCES " + LOOKUP_TABLE + "(" + LOOKUP_ID + ")," \
                  + DATA_VALUE + " TEXT NOT NULL" \
                                 ")"
        self.execute(command=command)

    def create_index(self):
        logger.info("Creating indices on data and lookup tables")

        #b-tree index
        create_index_statement_lookup = "CREATE INDEX " + LOOKUP_FIELD + \
                                        " ON " + LOOKUP_TABLE + \
                                        " (" + LOOKUP_FIELD + ");"
        create_index_statement_data = "CREATE INDEX " + DATA_LOOKUP_ID + \
                                      " ON " + DATA_TABLE + \
                                      " (" + DATA_LOOKUP_ID + ");"

        #Hash index
        # create_index_statement_lookup = "CREATE INDEX " + LOOKUP_FIELD +\
        #                                 " ON " + LOOKUP_TABLE + \
        #                                 " USING HASH" + \
        #                                 " (" + LOOKUP_ID + ");"
        # create_index_statement_data = "CREATE INDEX " + DATA_LOOKUP_ID + \
        #                               " ON " + DATA_TABLE + \
        #                               " USING HASH" + \
        #                               " (" + DATA_LOOKUP_ID + ");"

        try:
            self.execute(command=create_index_statement_lookup)
            self.execute(command=create_index_statement_data)
        except Exception as error:
            logger.error("Ignoring error during Index operation {0}".format(error))

    def drop_table(self):
        logger.info("Dropping lookup_table")
        self.execute("DROP TABLE lookup_table CASCADE;")

        logger.info("Dropping data_table")
        self.execute("DROP TABLE data_table;")

    def preprocessing(self):
        self.jmapper_util.build_memory_lookup()
