import logging
import pathlib

from .config import ConfigService
from .docs import DocumentProcessorFactory, DocumentReaderFactory, DocumentStore, DocWriter
from .index import IndexerFactory
from .topics import TopicReaderFactory, TopicProcessorFactory, QueryWriter
from .rerank import RerankFactory
from .retrieve import ResultsWriter, RetrieverFactory
from .score import QrelsReaderFactory, Scorer
from .util.file import delete_dir

LOGGER = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, config_filename, verbose=False, overrides=None):
        self.setup_logging(verbose)

        config_service = ConfigService(overrides)
        conf = config_service.read_config(config_filename)
        self.prepare_config(conf)

        if conf['overwrite'] and pathlib.Path(conf['path']).exists():
            LOGGER.debug("Deleting %s", conf['path'])
            delete_dir(conf['path'])

        self.doc_store = DocumentStore(conf['document_store']['path'])

        docs_conf = conf['documents']
        self.doc_reader = DocumentReaderFactory.create(docs_conf['input'])
        docs_process_conf = docs_conf['process']
        self.doc_processor = DocumentProcessorFactory.create(docs_process_conf, self.doc_reader, self.doc_store)
        self.doc_writer = DocWriter(docs_conf['save'], self.doc_processor)

        index_conf = conf['index']
        self.indexer = IndexerFactory.create(index_conf, self.doc_writer)
        self.stage1 = self.indexer

        topics_conf = conf['topics']
        self.topic_reader = TopicReaderFactory.create(topics_conf['input'])
        topics_process_conf = topics_conf['process']
        self.topic_processor = TopicProcessorFactory.create(topics_process_conf, self.topic_reader)
        self.query_writer = QueryWriter(topics_conf['save'], self.topic_processor)

        retrieve_conf = conf['retrieve']
        retrieve_conf['input'] = index_conf['save']
        self.retriever = RetrieverFactory.create(retrieve_conf, self.query_writer)
        self.retrieve_writer = ResultsWriter(retrieve_conf['save'], self.retriever)

        rerank_conf = conf['rerank']
        self.reranker = RerankFactory.create(rerank_conf, self.retrieve_writer, self.doc_store)
        self.rerank_writer = ResultsWriter(rerank_conf['save'], self.reranker)

        score_conf = conf['score']
        self.qrels = QrelsReaderFactory.create(score_conf['input']).read()
        self.scorer = Scorer(score_conf, self.rerank_writer, self.qrels)
        self.stage2 = self.scorer

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
                if 'save' in c:
                    c['save'] = str(base / c['save'])
        conf['document_store']['path'] = str(base / conf['document_store']['path'])

    def run(self):
        LOGGER.info("Starting processing of documents")
        doc_count = 0
        for _ in self.stage1:
            doc_count += 1
        self.stage1.end()
        LOGGER.info("Ingested %s documents", doc_count)

        LOGGER.info("Starting processing of topics")
        topic_count = 0
        for _ in self.stage2:
            topic_count += 1
        LOGGER.info("Processed %s topics", topic_count)
        LOGGER.info("System output available at %s", self.rerank_writer.path)
        self.stage2.end()
