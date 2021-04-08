import argparse

from pipeline import System


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SCALE 2021 Pipeline")
    parser.add_argument("config", help="Configuration file for the run")
    parser.add_argument("-v", dest="verbose", action="store_true", help="Increase verbosity of logger")
    parser.add_argument("--set", metavar="KEY=VALUE", nargs="+", help="Key-value pairs of parameters to override")
    args = parser.parse_args()

    system = System(args.config, args.verbose, args.set)
    system.run()
