import argparse

from patapsco import PatapscoError, Scorer
from patapsco import QrelsReaderFactory
from patapsco import ConfigPreprocessor
from patapsco import TrecResultsReader


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Produces scores from a qrels file and system output")
    parser.add_argument("config", help="Configuration file for the run")
    parser.add_argument("qrels", help="Qrels file for the run")
    parser.add_argument("system_output", help="System output for the run")
    args = parser.parse_args()

    try:
        #qrel_map = {"format": 'trec', "path": args.qrels}
        # TODO Want to use ConfigPreprocess but what about overrides
        conf = ConfigPreprocessor.process(args.config, overrides=None)
        #qrels = QrelsReaderFactory.create(qrel_map).read()
        ## testing config preprocessor
        ###scorer = Scorer(conf.score, qrels)
        # TODO - How to treat this as a results object
        ###scorer.process(args.system_output)
        ###print(scorer)
        ###scorer.end()
        print('Runs!!')
    except PatapscoError as error:
        print(f"Error: {error}")
