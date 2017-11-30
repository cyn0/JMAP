from base_db import BaseDB
import logging
import json

logger = logging.getLogger(__name__)

JSON_TABLE = "json_table"
JSON_ID = "json_id"
JSON_DATA = "json_data"

class JsonUtil(BaseDB):
    def __init__(self):
        super(JsonUtil, self).__init__()


    def create_json_table(self):
        logger.info("Creating json table if not exists")
        command = "CREATE TABLE IF NOT EXISTS " \
                  + JSON_TABLE + " (" \
                  + JSON_ID + " SERIAL," \
                  + JSON_DATA + " jsonb NOT NULL" \
                ")"
        self.execute(command=command)

    def create_index(self):
        logger.info("Creating indices on json table")

        create_index_statement = "CREATE UNIQUE INDEX " + JSON_DATA + " ON " + JSON_TABLE + " (" + JSON_ID + ");"

        try:
            self.execute(command=create_index_statement)
        except Exception as error:
            logger.error("Ignoring error during Index operation {0}".format(error))

    def insert_json(self, jObject):
        logger.info("Inserting {0}".format(jObject))
        insert_statement = "INSERT INTO " + JSON_TABLE + "(json_data) VALUES(%s) RETURNING json_id;"

        self.execute(insert_statement, (json.dumps(jObject),))

    def update_json(self, key, value):
        key = "{" + key + "}"
        value = '"'+ value +'"'
        
        #UPDATE json_table SET json_data = jsonb_set(json_data, '{fabk}', '"my-other-name"');
        update_statement = "UPDATE json_table SET json_data = jsonb_set(json_data, %s, %s);"
        self.execute(update_statement, (key, value))

    def drop_table(self):
        logger.info("Dropping json_table")
        self.execute("DROP TABLE json_table;")

    def preprocessing(self):
        pass
