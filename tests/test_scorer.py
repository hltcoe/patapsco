import pathlib

import pytest

from patapsco.schema import ScoreInputConfig, ScoreConfig
from patapsco.score import *


def test_at_symbol_mapping():
    config = ScoreConfig(
        input=ScoreInputConfig(path="test"),
        metrics=['map', 'P@20'] 
    )
    scorer = Scorer(config, qrels=None)
    assert scorer.config.metrics[0] == "map"
    assert scorer.config.metrics[1] == "P_20"    

@pytest.mark.skip(reason="not ready")
def test_ndcg_prime():
    # TODO complete testing for ndcg_prime
    config = ScoreConfig(
        input=ScoreInputConfig(path="test"),
        metrics=["ndcg'"] 
    )
    # create fake qrels to test ndcg', load them, pass to constructor
    scorer = Scorer(config, qrels=None)
    scorer.run = []  # fake results (could be in a file or entered here
    results = scorer.calc_ndcg_prime()
    #assert something
