import logging
import pathlib

from .config import load_config
from .core import DocWriter, TopicWriter, ResultsWriter, ResultsAccumulator
from .input import DocumentReaderFactory, DocumentStore, TopicReaderFactory, QrelsReaderFactory
from .index import IndexerFactory
from .rerank import RerankFactory
from .retrieve import RetrieverFactory
from .score import Scorer
from .text import DocumentProcessor, TopicProcessor

LOGGER = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, config_filename, verbose=False):
        self.setup_logging(verbose)

        config = load_config(config_filename)
        self.prepare_config(config)

        topic_config = config['input']['topics']
        self.topic_reader = TopicReaderFactory.create(topic_config)
        topic_process_config = config['query_process']
        self.topic_processor = TopicProcessor(topic_process_config)
        self.topic_writer = TopicWriter(topic_process_config['output'])

        doc_config = config['input']['documents']
        self.doc_reader = DocumentReaderFactory.create(doc_config)
        doc_process_config = config['document_process']
        self.doc_processor = DocumentProcessor(doc_process_config)
        self.doc_writer = DocWriter(doc_process_config['output'])
        self.doc_store = DocumentStore()

        qrels_config = config['input']['qrels']
        self.qrels = QrelsReaderFactory.create(qrels_config).read()
        self.accumulator = ResultsAccumulator()

        score_config = config['score']
        self.scorer = Scorer(score_config)

        index_config = config['index']
        self.indexer = IndexerFactory.create(index_config)

        retrieve_config = config['retrieve']
        retrieve_config['input'] = index_config['output']
        self.retriever = RetrieverFactory.create(retrieve_config)
        self.retrieve_writer = ResultsWriter(retrieve_config['output'])

        rerank_config = config['rerank']
        self.reranker = RerankFactory.create(rerank_config, self.doc_store)
        self.rerank_writer = ResultsWriter(rerank_config['output'])

    @staticmethod
    def setup_logging(verbose):
        log_level = logging.DEBUG if verbose else logging.INFO
        logger = logging.getLogger('pipeline')
        logger.setLevel(log_level)
        console = logging.StreamHandler()
        console.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console.setFormatter(formatter)
        logger.addHandler(console)

    def prepare_config(self, config):
        base = pathlib.Path(config['path'])
        for conf in config.values():
            if isinstance(conf, dict):
                if 'output' in conf:
                    conf['output'] = str(base / conf['output'])

    def run(self):
        LOGGER.info("Starting processing of documents")
        for doc in self.doc_reader:
            doc = self.doc_processor.run(doc)
            self.doc_writer.write(doc)
            self.indexer.index(doc)
        self.indexer.close()

        LOGGER.info("Starting processing of topics")
        results = {}
        for topic in self.topic_reader:
            topic = self.topic_processor.run(topic)
            self.topic_writer.write(topic)

            topic_results = self.retriever.retrieve(topic.id, topic.title)
            self.retrieve_writer.write(topic_results)
            results[topic.id] = topic_results

            topic_results = self.reranker.rerank(topic.title, topic_results)
            self.rerank_writer.write(topic_results)
            self.accumulator.add(topic_results)
        self.topic_writer.close()
        self.retrieve_writer.close()
        self.rerank_writer.close()
        LOGGER.info("Results available at %s", self.rerank_writer.path)
        self.scorer.score(self.qrels, self.accumulator.run)
