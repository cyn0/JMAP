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

    def insert_json(self, jObject):
        logger.info("Inserting {0}".format(jObject))
        insert_statement = "INSERT INTO " + JSON_TABLE + "(json_data) VALUES(%s) RETURNING json_id;"

        self.execute(insert_statement, (json.dumps(jObject),))
