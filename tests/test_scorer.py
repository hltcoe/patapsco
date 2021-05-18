import pathlib
import tempfile

import pytest

from patapsco.results import JsonResultsReader
from patapsco.schema import ScoreInputConfig, ScoreConfig
from patapsco.score import *
from patapsco.util.formats import parse_qrels


def test_at_symbol_mapping():
    config = ScoreConfig(
        input=ScoreInputConfig(path="test"),
        metrics=['map', 'P@20']
    )
    scorer = Scorer("test", config, qrels=None)
    assert scorer.metrics[0] == "map"
    assert scorer.metrics[1] == "P_20"


def test_pytrec_eval():
    temp_dir = pathlib.Path(tempfile.mkdtemp())

    # score the results
    base_directory = pathlib.Path(__file__).parent / 'scoring_files'
    qrels_path = base_directory / 'qrels.txt'
    results_path = base_directory / 'results.jsonl'
    config = ScoreConfig(
        input=ScoreInputConfig(path=str(qrels_path)),
        metrics=['map', 'ndcg', 'recall@100']
    )
    qrels_data = next(parse_qrels(qrels_path))
    scorer = Scorer(str(temp_dir), config, qrels=qrels_data)
    results_iter = JsonResultsReader(results_path)
    for r in results_iter:
        scorer.process(r)
    scorer.end()

    # check that the scores.txt file is correct
    scores_path = temp_dir / 'scores.txt'
    data = scores_path.read_text().split("\n")
    assert data[0].split()[0] == 'map'
    assert data[1].split()[0] == 'ndcg'
    assert data[2].split()[0] == 'recall_100'


def test_unknown_metric():
    directory = pathlib.Path(__file__).parent / 'scoring_files'
    qrels_path = directory / 'qrels.txt'
    run_path = directory / 'run.txt'
    results_path = directory / 'results.jsonl'
    config = ScoreConfig(
        input=ScoreInputConfig(path=str(run_path)),
        metrics=["ndcgg"]
    )
    qrels_iter = parse_qrels(str(qrels_path))
    qrels = next(qrels_iter)
    scorer = Scorer("test", config, qrels=qrels)
    results_iter = JsonResultsReader(results_path)
    for r in results_iter:
        scorer.process(r)
    with pytest.raises(ValueError) as e:
        scorer.end()


def test_ndcg_prime():
    directory = pathlib.Path(__file__).parent / 'scoring_files'
    qrels_path = directory / 'qrels.txt'
    results_path = directory / 'results.jsonl'
    config = ScoreConfig(
        input=ScoreInputConfig(path=str(qrels_path)),
        metrics=["ndcg'"]
    )
    qrels_iter = parse_qrels(str(qrels_path))
    qrels = next(qrels_iter)
    scorer = Scorer("test", config, qrels=qrels)
    results_iter = JsonResultsReader(results_path)
    for r in results_iter:
        scorer.process(r)
    results = scorer.calc_ndcg_prime()
    assert results['2']["ndcg'"] == 1
