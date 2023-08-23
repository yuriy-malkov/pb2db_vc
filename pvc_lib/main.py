import logging
import sys
from argparse import ArgumentParser


def add_db_credentials(sub_parsers):
    db_parser = sub_parsers.add_parser()


def main():
    arg_parser = ArgumentParser(description="Command line utility to help with pipeline actions")
    arg_parser.add_argument("--log-level", help="Sets the log level for the run",
                            choices=["DEBUG", "INFO", "WARN", "ERROR"], default="INFO")
    sub_parsers = arg_parser.add_subparsers(required=True)

    # Add the subparsers
    add_db_credentials(sub_parsers)


    # Finally parse the arguments
    args = arg_parser.parse_args()

    # Set the logging levels
    logging.basicConfig(level=getattr(logging, args.log_level), stream=sys.stdout)

    # In each subparser we set a 'func' which acts as an entrypoint for the CLI
    # Here we will call the respective subparser
    args.func(args)


if __name__ == "__main__":
    main()
