import logging
import random

from .config import BaseConfig
from .util import ComponentFactory, trec
from .util.file import GlobFileGenerator

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
        self.qrels_iter = GlobFileGenerator(config.path, trec.parse_qrels)

    def read(self):
        data = {}
        for qrels in self.qrels_iter:
            data = {**data, **qrels}
        return data


class Scorer:
    def __init__(self, config):
        self.config = ScorerConfig(**config)

    def score(self, qrels, run):
        for metric in self.config.metrics:
            LOGGER.info(f"{metric} = {random.random()}")
