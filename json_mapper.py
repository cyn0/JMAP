import logging
import optparse
import sys
from core.schema_builder import build_schema,delete_tables
from util import run_sample_tests, insert_multiple_json, random_read, update_concurrency_test

def initialise_service():
    logging.basicConfig(format='[%(asctime)s %(levelname)s]: %(message)s [%(filename)s:%(lineno)s - %(funcName)s]',
                        level=logging.DEBUG)

    logging.info("Initialising...")
    build_schema()

def start_service():
    pass

def main():
    initialise_service()

def stop_service():
    # delete_tables()
    pass

if __name__ == '__main__':
    cli_parser = optparse.OptionParser(usage="usage: %prog [options]")
    cli_parser.add_option(
        "-t",
        "--run-tests",
        action="store",
        type="int",
        dest="test_case",
        default=1,
        metavar="NUMBER",
        help="Which test to run?\n \
              1. Insert json using JMapper and default Postgres implementation \n \
              2. Random reads using JMapper and default Postgres implementation\n \
              3. Random updates using JMapper and default Postgres implementation")

    cli_parser.add_option(
        "-p",
        "--process",
        action="store",
        type="int",
        dest="num_process",
        default=1,
        metavar="NUMBER",
        help="How many concurrent process to start")

    (options, args) = cli_parser.parse_args()

    if args:
        print "Unrecognized option (use -h for help)"
        sys.exit(1)

    main()

    if options.test_case == 1:
        run_sample_tests(options.num_process)
    elif options.test_case == 2:
        random_read(options.num_process)
    elif options.test_case == 3:
        update_concurrency_test(options.num_process)

    stop_service()
