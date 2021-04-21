import copy
import random

from .config import BaseConfig, Optional, PathConfig
from .pipeline import Task
from .retrieve import Results
from .util import ComponentFactory


class RerankInputConfig(BaseConfig):
    """Configuration of optional rerank inputs"""
    db: PathConfig
    results: Optional[PathConfig]


class RerankConfig(BaseConfig):
    """Configuration for the rerank task"""
    input: RerankInputConfig
    name: str
    embedding: str
    output: PathConfig


class RerankFactory(ComponentFactory):
    classes = {
        'pacrr': 'MockReranker',
    }
    config_class = RerankConfig


class Reranker(Task):
    """Rerank interface"""

    def __init__(self, config, db):
        """
        Args:
            config (RerankConfig): Configuration parameters
            db (DocumentDatabase): Document database
        """
        super().__init__()
        self.config = config
        self.db = db

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
        # retrieve documents and pop one to exercise db
        docs = [self.db[result.doc_id] for result in new_results]
        docs.pop()
        random.shuffle(new_results)
        return Results(results.query, 'MockReranker', new_results)