import argparse

from patapsco import Runner, PatapscoError


def main():
    parser = argparse.ArgumentParser(description="SCALE 2021 Pipeline")
    parser.add_argument("config", help="Configuration file for the run")
    parser.add_argument("-v", dest="verbose", action="store_true", help="Increase verbosity of logger")
    parser.add_argument("--set", metavar="KEY=VALUE", nargs="+", help="Key-value pairs of parameters to override")
    args = parser.parse_args()

    try:
        runner = Runner(args.config, args.verbose, args.set)
        runner.run()
    except PatapscoError as error:
        print(f"Error: {error}")