import logging
from core.schema_builder import build_schema

def initialise_service():
    logging.basicConfig(format='[%(asctime)s %(levelname)s]: %(message)s [%(filename)s:%(lineno)s - %(funcName)s]',
                        level=logging.DEBUG)

    logging.info("Initialising...")
    build_schema()

def start_service():
    pass

def main():
    initialise_service()

    from util import insert_sample_json
    insert_sample_json()


if __name__ == '__main__':
    main()
