import pathlib

import pytest

from patapsco.results import TrecResultsReader
from patapsco.schema import ScoreInputConfig, ScoreConfig
from patapsco.score import *
from patapsco.util.trec import parse_qrels


def test_at_symbol_mapping():
    config = ScoreConfig(
        input=ScoreInputConfig(path="test"),
        metrics=['map', 'P@20'] 
    )
    scorer = Scorer("test", config, qrels=None)
    assert scorer.metrics[0] == "map"
    assert scorer.metrics[1] == "P_20"

@pytest.mark.skip(reason="should test open 'test' dir and look at output?")
def test_pytrec_eval():
    directory = pathlib.Path(__file__).parent / 'scoring_files'
    qrels_path = directory / 'qrels.txt'
    run_path = directory / 'run.txt'
    config = ScoreConfig(
        input=ScoreInputConfig(path=str(run_path)),
        metrics=['map', 'ndcg', 'recall@100']
    )
    qrels_iter = parse_qrels(str(qrels_path))
    qrels = next(qrels_iter)
    scorer = Scorer("test", config, qrels=qrels)
    results_iter = TrecResultsReader(str(run_path))
    for r in results_iter:
        scorer.process(r)
    scorer.end()


def test_unknown_metric():
    directory = pathlib.Path(__file__).parent / 'scoring_files'
    qrels_path = directory / 'qrels.txt'
    run_path = directory / 'run.txt'
    config = ScoreConfig(
        input=ScoreInputConfig(path=str(run_path)),
        metrics=["ndcgg"]
    )
    qrels_iter = parse_qrels(str(qrels_path))
    qrels = next(qrels_iter)
    scorer = Scorer("test", config, qrels=qrels)
    results_iter = TrecResultsReader(str(run_path))
    for r in results_iter:
        scorer.process(r)
    with pytest.raises(Exception) as e:
        results = scorer.end()

def test_ndcg_prime():
    directory = pathlib.Path(__file__).parent / 'scoring_files'
    qrels_path = directory / 'qrels.txt'
    run_path = directory / 'run.txt'
    config = ScoreConfig(
        input=ScoreInputConfig(path=str(run_path)),
        metrics=["ndcg'"]
    )
    qrels_iter = parse_qrels(str(qrels_path))
    qrels = next(qrels_iter)
    scorer = Scorer("test", config, qrels=qrels)
    results_iter = TrecResultsReader(str(run_path))
    for r in results_iter:
        scorer.process(r)
    results = scorer.calc_ndcg_prime()
    assert results['2']["ndcg'"] == 1

