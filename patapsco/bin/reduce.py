import argparse
import sys
import traceback

from patapsco import JobType, Runner, PatapscoError, __version__


def main():
    parser = argparse.ArgumentParser(description="Patapsco grid reducer")
    parser.add_argument("config", help="Configuration file for the run")
    parser.add_argument("-d", "--debug", action="store_true", help="Include debug information in logging")
    parser.add_argument("-v", "--version", action="version", version=f"Patapsco {__version__}")
    parser.add_argument("--stage", type=int, required=True, choices={1, 2}, help="Pipeline stage")
    args = parser.parse_args()

    parallel_args = {
        'stage': args.stage
    }
    try:
        runner = Runner(args.config, debug=args.debug, job_type=JobType.REDUCE, **parallel_args)
        runner.run()
    except PatapscoError as error:
        if args.debug:
            traceback.print_exc()
        else:
            print(f"Error: {error}")
        sys.exit(-1)


if __name__ == '__main__':
    main()
