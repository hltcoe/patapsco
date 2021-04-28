import pathlib
import patapsco.score as score
from patapsco import ConfigPreprocessor
from patapsco import QrelsReaderFactory
from patapsco import TrecResultsReader
from patapsco.util.trec import *
import patapsco.config as config


def test_at_symbol_mapping():
    directory = pathlib.Path(__file__).parent / 'config_files'
    path = directory / 'full_config.yml'
    conf = ConfigPreprocessor.process(path, overrides=None)
    scorer = score.Scorer(conf.score, qrels=None)
    assert scorer.config.metrics[0] == "map"
    assert scorer.config.metrics[1] == "P_20"    

def test_ndcg_prime():
    # TODO complete testing for ndcg_prime
    directory = pathlib.Path(__file__).parent / 'trec_files'
    qrel_file = directory / 'qrels_2020'
    results = directory / 'results.txt'
    path = pathlib.Path(__file__).parent / 'config_files' / 'full_config.yml'
    qrel_map = {"format": 'trec', "path": str(qrel_file)}
    qrels = QrelsReaderFactory.create(qrel_map).read()
    #qrels_iter = parse_qrels(str(qrel_file))
    conf = ConfigPreprocessor.process(path, overrides=None)
    run = TrecResultsReader(results)
    scorer = score.Scorer(conf.score, qrels)
    for r in run:
        scorer.process(r)
    print(scorer.calc_ndcg_prime())

test_ndcg_prime()
