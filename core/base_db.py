from abc import ABCMeta, abstractmethod
import psycopg2
import jsoncfg
import logging

logger = logging.getLogger(__name__)

class BaseDB(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self.config = jsoncfg.load_config('configs.cfg')

    @abstractmethod
    def insert_json(self, jObject):
        pass

    def get_connection(self):
        db_config = self.config['db']
        connection = psycopg2.connect(database=db_config.name(),
                                      user=db_config.user(),
                                      password=db_config.password(),
                                      host=db_config.host(),
                                      port=db_config.port_num())
        return connection

    def execute(self, command, params=None):
        connection = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor()

            logger.info("About to execute command: {0}".format(command))
            if params is not None:
                cursor.execute(command, params)
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

    def __str__(self):
        return ""