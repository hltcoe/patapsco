import collections
import csv
import logging
import random

from .config import BaseConfig
from .util import ComponentFactory

LOGGER = logging.getLogger(__name__)


class InputConfig(BaseConfig):
    name: str = "trec"
    path: str


class ScorerConfig(BaseConfig):
    metrics: list = ['map']
    input: InputConfig


class QrelsReaderFactory(ComponentFactory):
    classes = {
        'trec': 'TrecQrelsReader'
    }
    config_class = InputConfig


class TrecQrelsReader:
    def __init__(self, config):
        self.path = config.path

    def read(self):
        with open(self.path, 'r') as fp:
            reader = csv.reader(fp, delimiter=' ')
            qrels = collections.defaultdict(dict)
            for row in reader:
                qrels[row[0]][row[2]] = int(row[3])
            return qrels


class Scorer:
    def __init__(self, config):
        self.config = ScorerConfig(**config)

    def score(self, qrels, run):
        for metric in self.config.metrics:
            LOGGER.info(f"{metric} = {random.random()}")
