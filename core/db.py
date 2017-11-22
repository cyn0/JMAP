import psycopg2
import jsoncfg
import logging

logger = logging.getLogger(__name__)
config = jsoncfg.load_config('configs.cfg')

LOOKUP_TABLE = "lookup_tablee"
LOOKUP_ID = "lookup_id"
LOOKUP_FIELD = "lookup_fied"

DATA_TABLE = "data_table"
DATA_OBJECT_ID = "data_object_id"
DATA_LOOKUP_ID = "data_lookup_id"
DATA_VALUE = "data_value"

def execute(command, dict=None):
    connection = None
    try:
        db_config = config['db']
        connection = psycopg2.connect(database=db_config.name(),
                                user=db_config.user(),
                                password=db_config.password(),
                                host=db_config.host(),
                                port=db_config.port_num())
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


def create_lookup_table():
    logger.info("Creating lookup table if not exists")
    command = "CREATE TABLE IF NOT EXISTS " \
                + LOOKUP_TABLE + " (" \
                + LOOKUP_ID  + " CHARACTER(255) NOT NULL, " \
                + LOOKUP_FIELD + " CHARACTER(255) NOT NULL, " \
                + "PRIMARY KEY (" + LOOKUP_ID + ", " + LOOKUP_FIELD +"))"

    execute(command=command)


def create_string_table():
    logger.info("Creating lookup table if not exists")
    command = "CREATE TABLE IF NOT EXISTS " \
              + DATA_TABLE + " (" \
              + DATA_OBJECT_ID + " INTEGER NOT NULL," \
              + DATA_LOOKUP_ID + " INTEGER NOT NULL REFERENCES " + LOOKUP_TABLE + "(" + LOOKUP_ID + ")," \
              + DATA_VALUE + " CHARACTER(255) NOT NULL" \
                ")"
    execute(command=command)

def insert_lookup_table(valuedict):
    logger.info("Inserting into lookup table")
    command = "INSERT INTO " + LOOKUP_TABLE +"("+ LOOKUP_ID + ", " + LOOKUP_FIELD + ")" \
              " VALUES (%(id)s, %(field)s)"
    execute(command=command, dict=valuedict)

def select_all_lookup():
    logger.info("Selecting from lookup table")
    command = "SELECT * FROM " + LOOKUP_TABLE
    execute(command=command)