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
DATA_LOOKUP_FIELD = "data_lookup_fied"
DATA_VALUE = "data_value"

class JMAPPER(BaseDB):
    def __init__(self):
        super(JMAPPER, self).__init__()
        self.jmapper_util = JMapperUtil(self)

    def read_lookup(self, filter):
        statement = "SELECT * from " + LOOKUP_TABLE + ";"
        return self.execute(statement, returnsResult=True)

    def get_json(self, condition=None, condition_value=None):
        filter_statement = "SELECT data_object_id from " + DATA_TABLE + " WHERE data_lookup_fied=%s AND data_value=%s;"
        #select_statement = "SELECT data_lookup_fied, data_value from " + DATA_TABLE + " WHERE data_object_id=%s;"
        select_statement = "SELECT data_object_id, data_lookup_fied, data_value from " + DATA_TABLE + " WHERE data_object_id IN %s;"
        connection = None

        try:
            start_time_1 = timeit.default_timer()
            connection = self.get_connection()
            cursor = connection.cursor()
            cursor.execute(filter_statement, (condition, condition_value))
            data_object_id = cursor.fetchall()

            ids = tuple([tupl[0] for tupl in data_object_id ])
            cursor.execute(select_statement, (ids, ))
            result = cursor.fetchall()
            # cursor.close()
            connection.commit()

            jsonObject = {}
            for item in result:
                object_id = item[0]
                key = item[1]
                value = item[2]

                if object_id not in jsonObject:
                    jsonObject[object_id] = {}

                curr_json = jsonObject[object_id]
                levels = key.split(".")
                temp=curr_json

                level_count= len(levels)
                for i in range(level_count):
                    curr_level = levels[i]
                    if levels[i] not in temp:
                        if i == level_count - 1:
                            temp[curr_level] = value
                        else:
                            temp[curr_level] = {}

                    temp = temp[curr_level]

            result = [jsonObject[key] for key in jsonObject]
            elapsed_1 = timeit.default_timer() - start_time_1

            with open('read_time_jmapper.csv', 'a') as file:
                    file.write(str(elapsed_1) + '\n')
            return result

        except (Exception, psycopg2.DatabaseError) as error:
            logger.error("Error during DB operation {0}".format(error))
            raise

        finally:
            if connection is not None:
                connection.close()


    def _insert_json_db(self, jsonList):
        insert_statement_lookup = "INSERT INTO " + LOOKUP_TABLE + "(lookup_id, lookup_fied,lookup_fied_level) VALUES(%s, %s, %s);"
        insert_statement_data_with_id = "INSERT INTO " + DATA_TABLE + "(data_object_id, data_lookup_id, data_lookup_fied, data_value) VALUES(%s, %s, %s, %s);"
        insert_statement_data = "INSERT INTO " + DATA_TABLE + "(data_lookup_id, data_lookup_fied, data_value) VALUES(%s, %s, %s) RETURNING data_object_id;"

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
                                       (data_object_id, item["data_lookup_id"], item["data_lookup_fied"], item["data_value"]))
                    else:

                        cursor.execute(insert_statement_data, (item["data_lookup_id"], item["data_lookup_fied"], item["data_value"]))
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
            lookup_field =  self.jmapper_util.append_field_path(file_name_prefix, k)
            cursor.execute(statement, (lookup_field, ))

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
                flattenedList.append({"lookup_id": lookup_id, "lookup_field": lookup_field, "type": "lookup","lookup_fied_level": level})
            else:
                lookup_id = result[0]
                lookup_field = result[1]
                level = result[2]
                c_prefix = str(lookup_id)[:(level + 1) * 2]

            if isinstance(v, dict):
                self.flattenJson(v, level + 1, c_prefix, lookup_field, flattenedList)

            else:
                flattenedList.append({"data_lookup_id": lookup_id, "data_lookup_fied": lookup_field, "data_value": v, "type": "data"})

        cursor.close()

    def insert_json(self, jObject):
        flattenedList = []
        self.flattenJson(jObject, 0, "", None, flattenedList)
        self._insert_json_db(flattenedList)

    def update_json(self, keyPath, value, conditionPath = None, conditionValue = None, log_file_name = None):
       # select_lookupid_statement = "SELECT " + LOOKUP_ID + ", " + LOOKUP_FIELD_LEVEL +" from " + LOOKUP_TABLE + " WHERE " + LOOKUP_FIELD + "=%s"
        select_objectid_statement = "SELECT " + DATA_OBJECT_ID + " from " + DATA_TABLE + " WHERE " + DATA_LOOKUP_FIELD + "=%s AND "+ DATA_VALUE + " =%s"
        update_statement = "UPDATE " + DATA_TABLE + " SET "+ DATA_VALUE +" = %s WHERE data_lookup_fied = %s AND "+ DATA_OBJECT_ID +" IN %s;"
        
        connection = None
        try:
            #logger.info("Updating {0} value as {1}".format(keyPath, value))
            start_time_1 = timeit.default_timer()
            connection = self.get_connection()
            cursor = connection.cursor()

            cursor.execute(select_objectid_statement, (conditionPath, conditionValue))

            data_object_id = cursor.fetchall()

            print select_objectid_statement;
            print update_statement;



            ids = tuple([tupl[0] for tupl in data_object_id ])
            print "data_object_id", data_object_id
            print "ids", ids
            if ids is None:
                return
            cursor.execute(update_statement, (value, keyPath, ids))

            connection.commit()

            elapsed_1 = timeit.default_timer() - start_time_1
            cursor.close()
            logger.info("Updating {0} value as {1}. Time taken to Update: JMapper: {2}".format(keyPath, value, elapsed_1))

            if log_file_name is not None:
                with open(log_file_name + '.csv', 'a') as file:
                    file.write(str(elapsed_1) + '\n')

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
                  + DATA_LOOKUP_FIELD + " TEXT NOT NULL," \
                  + DATA_VALUE + " TEXT NOT NULL" \
                                 ")"
        self.execute(command=command)

    def create_index(self):
        logger.info("Creating indices on data and lookup tables")

        #b-tree index
        create_index_statement_lookup = "CREATE UNIQUE INDEX " + LOOKUP_FIELD + \
                                        " ON " + LOOKUP_TABLE + \
                                        " (" + LOOKUP_FIELD + ");"
        create_index_statement_data = "CREATE INDEX " + DATA_LOOKUP_ID + \
                                      " ON " + DATA_TABLE + \
                                      " (" + DATA_LOOKUP_ID + ");"
        create_index_statement_data_field = "CREATE  INDEX " + DATA_LOOKUP_FIELD + \
                                      " ON " + DATA_TABLE + \
                                      " (" + DATA_LOOKUP_FIELD + ");"
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
            self.execute(command=create_index_statement_data_field)
        except Exception as error:
            logger.error("Ignoring error during Index operation {0}".format(error))

    def drop_table(self):
        logger.info("Dropping lookup_table")
        self.execute("DROP TABLE lookup_table CASCADE;")

        logger.info("Dropping data_table")
        self.execute("DROP TABLE data_table;")

    def preprocessing(self):
        self.jmapper_util.build_memory_lookup()
