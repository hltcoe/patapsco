import copy
import random

from .config import BaseConfig
from .pipeline import Task
from .retrieve import Results
from .util import ComponentFactory


class RerankConfig(BaseConfig):
    name: str
    embedding: str
    save: str


class RerankFactory(ComponentFactory):
    classes = {
        'pacrr': 'MockReranker',
    }
    config_class = RerankConfig


class Reranker(Task):
    """Rerank interface"""

    def __init__(self, config, store):
        """
        Args:
            config (RerankConfig): Configuration parameters
            store (DocumentStore): Document store that works like dictionary
        """
        super().__init__()
        self.config = config
        self.store = store

    def process(self, results):
        """Rerank query results

        Args:
            results (Results)

        Returns:
            Results
        """
        pass


class MockReranker(Reranker):
    """Mock reranker for testing"""

    def process(self, results):
        new_results = copy.copy(results.results)
        random.shuffle(new_results)
        return Results(results.query, 'MockReranker', new_results)
