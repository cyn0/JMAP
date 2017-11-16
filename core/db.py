import psycopg2
import jsoncfg
import logging

logger = logging.getLogger(__name__)
config = jsoncfg.load_config('configs.cfg')

LOOKUP_TABLE = "lookup_table"
LOOKUP_ID = "lookup_id"
LOOKUP_FIELD = "lookup_fied"

def execute(command):
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

        cursor.execute(command)

        cursor.close()
        connection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        raise

    finally:
        if connection is not None:
            connection.close()


def create_lookup_table():
    logger.info("Creating lookup table if not exists")
    command = "CREATE TABLE IF NOT EXISTS " \
                + LOOKUP_TABLE + " (" \
                + LOOKUP_ID + " SERIAL PRIMARY KEY," \
                + LOOKUP_FIELD + " INTEGER NOT NULL" \
                ")"

    execute(command=command)

def create_string_table():
    logger.info("Creating lookup table if not exists")
    pass
