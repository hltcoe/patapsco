import argparse
import sys
import traceback

from patapsco import Runner, PatapscoError, __version__


def main():
    parser = argparse.ArgumentParser(description="SCALE 2021 Pipeline")
    parser.add_argument("config", help="Configuration file for the run")
    parser.add_argument("-d", "--debug", action="store_true", help="Include debug information in logging")
    parser.add_argument("-v", "--version", action="version", version=f"Patapsco {__version__}")
    parser.add_argument("-s", "--set", action='append', metavar="KEY=VALUE",
                        help="Key-value pair for overriding a parameter. Flag can be used more than once.")
    args = parser.parse_args()

    try:
        runner = Runner(args.config, debug=args.debug, overrides=args.set)
        runner.run()
    except PatapscoError as error:
        if args.debug:
            traceback.print_exc()
        else:
            print(f"Error: {error}")
        sys.exit(-1)


if __name__ == '__main__':
    main()
