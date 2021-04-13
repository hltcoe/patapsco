import logging
import pathlib

from .config import ConfigService
from .docs import DocumentProcessorFactory, DocumentReaderFactory, DocumentStore, DocWriter
from .index import IndexerFactory
from .pipeline import Pipeline, Task
from .rerank import RerankFactory
from .retrieve import ResultsWriter, RetrieverFactory
from .score import QrelsReaderFactory, Scorer
from .topics import TopicReaderFactory, TopicProcessorFactory, QueryWriter
from .util.file import delete_dir

LOGGER = logging.getLogger(__name__)


class System:
    def __init__(self, config_filename, verbose=False, overrides=None):
        self.setup_logging(verbose)

        config_service = ConfigService(overrides)
        conf = config_service.read_config(config_filename)
        self.prepare_config(conf)

        if 'overwrite' in conf and conf['overwrite'] and pathlib.Path(conf['path']).exists():
            LOGGER.debug("Deleting %s", conf['path'])
            delete_dir(conf['path'])

        self.doc_store = DocumentStore(conf['document_store']['path'])

        docs_conf = conf['documents']
        self.doc_reader = DocumentReaderFactory.create(docs_conf['input'])
        docs_process_conf = docs_conf['process']
        self.doc_processor = DocumentProcessorFactory.create(docs_process_conf, self.doc_store)
        self.doc_writer = DocWriter(docs_conf['save'])

        index_conf = conf['index']
        self.indexer = IndexerFactory.create(index_conf)
        self.stage1 = Pipeline(self.doc_processor | self.doc_writer | self.indexer)

        topics_conf = conf['topics']
        self.topic_reader = TopicReaderFactory.create(topics_conf['input'])
        topics_process_conf = topics_conf['process']
        self.topic_processor = TopicProcessorFactory.create(topics_process_conf)
        self.query_writer = QueryWriter(topics_conf['save'])

        retrieve_conf = conf['retrieve']
        retrieve_conf['input'] = index_conf['save']
        self.retriever = RetrieverFactory.create(retrieve_conf)
        self.retrieve_writer = ResultsWriter(retrieve_conf['save'])

        rerank_conf = conf['rerank']
        self.reranker = RerankFactory.create(rerank_conf, self.doc_store)
        self.rerank_writer = ResultsWriter(rerank_conf['save'])

        score_conf = conf['score']
        self.qrels = QrelsReaderFactory.create(score_conf['input']).read()
        self.scorer = Scorer(score_conf, self.qrels)
        self.stage2 = Pipeline(self.topic_processor | self.query_writer | self.retriever |
                               self.retrieve_writer | self.reranker | self.rerank_writer | self.scorer)

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
        LOGGER.info("Stage 1 pipeline: %s", self.stage1)
        LOGGER.info("Stage 2 pipeline: %s", self.stage2)

        LOGGER.info("Starting processing of documents")
        self.stage1.run(self.doc_reader)
        LOGGER.info("Ingested %s documents", self.stage1.count)

        LOGGER.info("Starting processing of topics")
        self.stage2.run(self.topic_reader)
        LOGGER.info("Processed %s topics", self.stage2.count)
        LOGGER.info("System output available at %s", self.rerank_writer.path)
