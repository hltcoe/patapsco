import argparse

from pipeline import Pipeline


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config", help="Config file for the runs")
    args = parser.parse_args()

    pipeline = Pipeline(args.config)
    pipeline.run()
