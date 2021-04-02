import copy
import random

from .config import BaseConfig
from .core import Result
from .error import ConfigError


class Reranker:
    """Rerank interface"""
    def __init__(self, config, store):
        """
        Args:
            config (RerankConfig): Configuration parameters
            store (DocumentStore): Document store that works like dictionary
        """
        self.config = config
        self.store = store

    def rerank(self, query, results):
        """Rerank a list of query results

        Args:
            query (str)
            results (list)

        Returns:
            list of Result
        """
        pass

    def close(self):
        """Close any files and release any resources"""
        pass


class MockReranker(Reranker):
    def rerank(self, query, results):
        results = copy.copy(results)
        random.shuffle(results)
        return [Result(v.query_id, v.doc_id, i, i, v.name) for i, v in enumerate(results)]


class RerankConfig(BaseConfig):
    name: str
    embedding: str
    output: str


class RerankFactory:
    classes = {
        'pacrr': MockReranker,
    }

    @classmethod
    def create(cls, config, store):
        config = RerankConfig(**config)
        if config.name not in cls.classes:
            raise ConfigError(f"Unknown reranker: {config.name}")
        return cls.classes[config.name](config, store)
