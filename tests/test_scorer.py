import pathlib
import tempfile

import pytest

from patapsco.results import JsonResultsReader
from patapsco.schema import ScoreInputConfig, ScoreConfig
from patapsco.score import *
from patapsco.util.file import delete_dir
from patapsco.util.formats import parse_qrels


class TestScorer:
    directory = (pathlib.Path(__file__).parent / 'scoring_files').absolute()

    def setup_method(self):
        self.temp_dir = pathlib.Path(tempfile.mkdtemp())
        self.qrels_path = self.directory / 'qrels.txt'
        self.results_path = self.directory / 'results.jsonl'
        self.qrels_data = next(parse_qrels(self.qrels_path))
        self.results_iter = JsonResultsReader(self.results_path)

    def teardown_method(self):
        delete_dir(self.temp_dir)

    def create_scorer(self, metrics):
        config = ScoreConfig(
            input=ScoreInputConfig(path=str(self.qrels_path)),
            metrics=metrics
        )
        return Scorer(str(self.temp_dir), config, qrels=self.qrels_data)

    def test_at_symbol_mapping(self):
        scorer = self.create_scorer(['map', 'P@20'])
        assert scorer.metrics[0] == "map"
        assert scorer.metrics[1] == "P_20"

    def test_ndcg_prime_standardization(self):
        scorer = self.create_scorer(['map', "ndcg'"])
        assert scorer.metrics[0] == "map"
        assert scorer.metrics[1] == "ndcg_prime"

    def test_unknown_metric(self):
        with pytest.raises(ConfigError):
            self.create_scorer(["ndcgg"])

    def test_pytrec_eval(self):
        # score the results
        scorer = self.create_scorer(['map', 'ndcg', 'recall@100'])
        for r in self.results_iter:
            scorer.process(r)
        scorer.end()

        # check that the scores.txt file is correct
        scores_path = self.temp_dir / 'scores.txt'
        data = scores_path.read_text().split("\n")
        assert data[0].split()[0] == 'map'
        assert data[1].split()[0] == 'ndcg'
        assert data[2].split()[0] == 'recall_100'

    def test_ndcg_prime(self):
        scorer = self.create_scorer(["ndcg'"])
        for r in self.results_iter:
            scorer.process(r)
        results = scorer._calc_ndcg_prime()
        assert results['2']["ndcg_prime"] == 1
