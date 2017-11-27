import logging
from core.schema_builder import build_schema,delete_tables

def initialise_service():
    logging.basicConfig(format='[%(asctime)s %(levelname)s]: %(message)s [%(filename)s:%(lineno)s - %(funcName)s]',
                        level=logging.DEBUG)

    logging.info("Initialising...")
    build_schema()

def start_service():
    pass

def main():
    initialise_service()

    from util import run_sample_tests
    run_sample_tests()

def stop_service():
    delete_tables()

if __name__ == '__main__':
    main()
    # stop_service()
