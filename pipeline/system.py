import logging
import pathlib

from .config import BaseConfig, ConfigService
from .docs import DocumentsConfig, DocumentStoreConfig, DocumentProcessorFactory, DocumentReaderFactory, \
    DocumentStore, DocWriter
from .index import IndexConfig, IndexerFactory
from .pipeline import Pipeline
from .rerank import RerankConfig, RerankFactory
from .retrieve import ResultsWriter, RetrieveConfig, RetrieverFactory
from .score import QrelsReaderFactory, ScoreConfig, Scorer
from .topics import TopicProcessorFactory, TopicReaderFactory, TopicsConfig, QueryWriter
from .util.file import delete_dir

LOGGER = logging.getLogger(__name__)


class RunConfig(BaseConfig):
    """Configuration for a run of the system"""
    path: str
    documents: DocumentsConfig
    document_store: DocumentStoreConfig
    topics: TopicsConfig
    index: IndexConfig
    retrieve: RetrieveConfig
    rerank: RerankConfig
    score: ScoreConfig


class System:
    def __init__(self, config_filename, verbose=False, overrides=None):
        self.setup_logging(verbose)

        config_service = ConfigService(overrides)
        conf_dict = config_service.read_config(config_filename)
        self.prepare_config(conf_dict)
        conf = RunConfig(**conf_dict)

        #if 'overwrite' in conf and conf['overwrite'] and pathlib.Path(conf['path']).exists():
        #    LOGGER.debug("Deleting %s", conf['path'])
        delete_dir(conf.path)

        self.doc_store = DocumentStore(conf.document_store.path)
        self.doc_reader = DocumentReaderFactory.create(conf.documents.input)
        self.doc_processor = DocumentProcessorFactory.create(conf.documents.process, self.doc_store)
        self.doc_writer = DocWriter(conf.documents.save)

        self.indexer = IndexerFactory.create(conf.index)
        self.stage1 = Pipeline(self.doc_processor | self.doc_writer | self.indexer)

        self.topic_reader = TopicReaderFactory.create(conf.topics.input)
        self.topic_processor = TopicProcessorFactory.create(conf.topics.process)
        self.query_writer = QueryWriter(conf.topics.save)

        self.retriever = RetrieverFactory.create(conf.retrieve)
        self.retrieve_writer = ResultsWriter(conf.retrieve.save)

        self.reranker = RerankFactory.create(conf.rerank, self.doc_store)
        self.rerank_writer = ResultsWriter(conf.rerank.save)

        self.qrels = QrelsReaderFactory.create(conf.score.input).read()
        self.scorer = Scorer(conf.score, self.qrels)
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

    @staticmethod
    def prepare_config(conf):
        base = pathlib.Path(conf['path'])
        # set path for components to be under the base directory of run
        for c in conf.values():
            if isinstance(c, dict):
                if 'save' in c:
                    c['save'] = str(base / c['save'])
        conf['document_store']['path'] = str(base / conf['document_store']['path'])
        # if input to retrieve is not set, we grab it from index
        if 'input' not in conf['retrieve']:
            conf['retrieve']['input'] = {'path': conf['index']['save']}

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
