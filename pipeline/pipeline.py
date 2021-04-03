import logging
import pathlib

from .config import ConfigService
from .core import DocWriter, ResultsWriter, ResultsAccumulator
from .doc import DocumentProcessorFactory, DocumentReaderFactory
from .index import DocumentStore, IndexerFactory
from .query import TopicReaderFactory, QueryProcessorFactory, QueryWriter
from .rerank import RerankFactory
from .retrieve import RetrieverFactory
from .score import QrelsReaderFactory, Scorer
from .util.file import delete_dir

LOGGER = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, config_filename, verbose=False, overrides=None):
        self.setup_logging(verbose)

        config_service = ConfigService()
        conf = config_service.read_config(config_filename)
        self.prepare_config(conf)

        if conf['overwrite'] and pathlib.Path(conf['path']).exists():
            LOGGER.debug("Deleting %s", conf['path'])
            delete_dir(conf['path'])

        topic_conf = conf['input']['topics']
        self.topic_reader = TopicReaderFactory.create(topic_conf)
        query_process_conf = conf['query_process']
        self.query_processor = QueryProcessorFactory.create(query_process_conf)
        self.query_writer = QueryWriter(query_process_conf['output'])

        doc_conf = conf['input']['documents']
        self.doc_reader = DocumentReaderFactory.create(doc_conf)
        doc_process_conf = conf['document_process']
        self.doc_processor = DocumentProcessorFactory.create(doc_process_conf)
        self.doc_writer = DocWriter(doc_process_conf['output'])
        self.doc_store = DocumentStore()

        qrels_conf = conf['input']['qrels']
        self.qrels = QrelsReaderFactory.create(qrels_conf).read()
        self.accumulator = ResultsAccumulator()

        score_conf = conf['score']
        self.scorer = Scorer(score_conf)

        index_conf = conf['index']
        self.indexer = IndexerFactory.create(index_conf)

        retrieve_conf = conf['retrieve']
        retrieve_conf['input'] = index_conf['output']
        self.retriever = RetrieverFactory.create(retrieve_conf)
        self.retrieve_writer = ResultsWriter(retrieve_conf['output'])

        rerank_conf = conf['rerank']
        self.reranker = RerankFactory.create(rerank_conf, self.doc_store)
        self.rerank_writer = ResultsWriter(rerank_conf['output'])

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

    def prepare_config(self, conf):
        base = pathlib.Path(conf['path'])
        for c in conf.values():
            if isinstance(c, dict):
                if 'output' in c:
                    c['output'] = str(base / c['output'])

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
            query = self.query_processor.run(topic)
            self.query_writer.write(query)

            query_results = self.retriever.retrieve(query.id, query.text)
            self.retrieve_writer.write(query_results)
            results[topic.id] = query_results

            query_results = self.reranker.rerank(topic.title, query_results)
            self.rerank_writer.write(query_results)
            self.accumulator.add(query_results)
        self.query_writer.close()
        self.retrieve_writer.close()
        self.rerank_writer.close()
        LOGGER.info("Results available at %s", self.rerank_writer.path)
        self.scorer.score(self.qrels, self.accumulator.run)
