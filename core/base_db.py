from abc import ABCMeta, abstractmethod
import psycopg2
import jsoncfg
import timeit
import logging

logger = logging.getLogger(__name__)

class BaseDB(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self.config = jsoncfg.load_config('configs.cfg')

    @abstractmethod
    def insert_json(self, jObject):
        pass

    @abstractmethod
    def update_json(self, key, value):
        pass

    def get_connection(self):
        db_config = self.config['db']
        connection = psycopg2.connect(database=db_config.name(),
                                      user=db_config.user(),
                                      password=db_config.password(),
                                      host=db_config.host(),
                                      port=db_config.port_num())
        return connection

    def execute(self, command, params=None, returnsResult=False):
        connection = None
        try:
            logger.info("About to execute command: {0}".format(command))
            start_time_1 = timeit.default_timer()
            connection = self.get_connection()
            cursor = connection.cursor()

            cursor.execute(command, params)
            # cursor.close()
            connection.commit()
            elapsed_1 = timeit.default_timer() - start_time_1

            logger.info("Time taken to Execute command {0}: {1}".format(command, elapsed_1))

            result = []
            if returnsResult:
                result = cursor.fetchall()
            cursor.close()
            return result

        except (Exception, psycopg2.DatabaseError) as error:
            logger.error("Error during DB operation {0}".format(error))
            raise

        finally:
            if connection is not None:
                connection.close()

    @abstractmethod
    def drop_table(self):
        pass

    @abstractmethod
    def create_index(self):
        pass

    def __str__(self):
        return ""