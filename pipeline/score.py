import collections
import csv
import logging
import random

from .config import BaseConfig
from .error import ConfigError

LOGGER = logging.getLogger(__name__)


class InputQrelsConfig(BaseConfig):
    format: str
    path: str


class QrelsReaderFactory:
    @classmethod
    def create(cls, config):
        config = InputQrelsConfig(**config)
        if config.format == "trec":
            return TrecQrelsReader(config.path)
        else:
            raise ConfigError(f"Unknown qrels format: {config.format}")


class TrecQrelsReader:
    def __init__(self, path):
        self.path = path

    def read(self):
        with open(self.path, 'r') as fp:
            reader = csv.reader(fp, delimiter=' ')
            qrels = collections.defaultdict(dict)
            for row in reader:
                qrels[row[0]][row[2]] = int(row[3])
            return qrels


class ScorerConfig(BaseConfig):
    metrics: list = ['map']


class Scorer:
    def __init__(self, config):
        self.config = ScorerConfig(**config)

    def score(self, qrels, run):
        for metric in self.config.metrics:
            LOGGER.info(f"{metric} = {random.random()}")
