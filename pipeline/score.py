import logging
import random

from .config import BaseConfig

LOGGER = logging.getLogger(__name__)


class ScorerConfig(BaseConfig):
    metrics: list = ['map']


class Scorer:
    def __init__(self, config):
        self.config = ScorerConfig(**config)

    def score(self, qrels, run):
        for metric in self.config.metrics:
            LOGGER.info(f"{metric} = {random.random()}")
