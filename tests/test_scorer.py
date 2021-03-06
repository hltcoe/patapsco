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
        self.scores_path = self.temp_dir / 'scores.txt'
        self.qrels_path = self.directory / 'qrels.txt'
        self.results_path = self.directory / 'results.txt'
        self.qrels_data = next(parse_qrels(self.qrels_path))
        self.results_iter = JsonResultsReader(self.results_path)

    def teardown_method(self):
        delete_dir(self.temp_dir)

    def create_scorer(self, metrics):
        config = ScoreInputConfig(path=str(self.qrels_path))
        return Scorer(config, metrics)

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
        scorer.score(self.results_path, self.scores_path)

        # check that the scores.txt file is correct
        data = self.scores_path.read_text().split("\n")
        assert data[0].split()[0] == 'map'
        assert data[1].split()[0] == 'ndcg'
        assert data[2].split()[0] == 'recall_100'

    def test_ndcg_prime(self):
        scorer = self.create_scorer(["ndcg'"])
        with open(self.results_path, 'r') as fp:
            system_output = pytrec_eval.parse_run(fp)
        results = scorer._calc_ndcg_prime(system_output)
        assert results['2']["ndcg_prime"] == 1

    def test_ndcg_prime_mixed(self):
        modified_qrels = self.directory / 'qrels_missing_doc.txt'

        def inner_create_scorer(metrics):
            config = ScoreInputConfig(path=str(modified_qrels))
            return Scorer(config, metrics)
        scorer = inner_create_scorer(["ndcg'"])
        with open(self.results_path, 'r') as fp:
            system_output = pytrec_eval.parse_run(fp)
        results = scorer._calc_ndcg_prime(system_output)
        assert results['2']["ndcg_prime"] == 0
