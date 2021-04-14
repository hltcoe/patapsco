import enum
import logging
import pathlib

from .config import BaseConfig, ConfigService, Optional
from .docs import DocumentsConfig, DocumentProcessorFactory, DocumentReaderFactory, \
    DocumentDatabaseFactory, DocWriter
from .error import ConfigError
from .index import IndexConfig, IndexerFactory
from .pipeline import Pipeline
from .rerank import RerankConfig, RerankFactory
from .retrieve import JsonResultsWriter, JsonResultsReader, TrecResultsWriter, RetrieveConfig, RetrieverFactory
from .score import QrelsReaderFactory, ScoreConfig, Scorer
from .topics import TopicProcessorFactory, TopicReaderFactory, TopicsConfig, QueryReader, QueryWriter
from .util.file import delete_dir, is_complete

LOGGER = logging.getLogger(__name__)


class RunConfig(BaseConfig):
    """Configuration for a run of the system"""
    path: str
    documents: Optional[DocumentsConfig]
    index: Optional[IndexConfig]
    topics: Optional[TopicsConfig]
    retrieve: Optional[RetrieveConfig]
    rerank: Optional[RerankConfig]
    score: Optional[ScoreConfig]


class Tasks(enum.Enum):
    """Tasks that make up the system pipelines"""
    DOCUMENTS = enum.auto()
    INDEX = enum.auto()
    TOPICS = enum.auto()
    RETRIEVE = enum.auto()
    RERANK = enum.auto()
    SCORE = enum.auto()


class RunConfigPreprocessor:
    """Performs advanced validation and preprocessing of run configuration"""

    @classmethod
    def process(cls, config_filename, overrides):
        config_service = ConfigService(overrides)
        conf_dict = config_service.read_config(config_filename)
        cls._validate(conf_dict)
        cls._update_relative_paths(conf_dict)
        cls._set_retrieve_input_path(conf_dict)
        cls._set_rerank_db_path(conf_dict)
        return RunConfig(**conf_dict)

    @staticmethod
    def _validate(conf_dict):
        if 'path' not in conf_dict:
            raise ConfigError("path is not set")

    @staticmethod
    def _set_retrieve_input_path(conf_dict):
        # if index location for retrieve is not set, we grab it from index config
        if 'input' not in conf_dict['retrieve'] or 'index' not in conf_dict['retrieve']['input']:
            if 'documents' in conf_dict and 'save' in conf_dict['index']:
                if 'input' not in conf_dict['retrieve']:
                    conf_dict['retrieve']['input'] = {}
                if 'db' not in conf_dict['retrieve']['input']:
                    conf_dict['retrieve']['input']['index'] = {'path': conf_dict['index']['save']}
            else:
                raise ConfigError("documents.index.save needs to be set for this run")

    @staticmethod
    def _set_rerank_db_path(conf_dict):
        # if db path for rerank is not set, we grab it from documents config
        if 'input' not in conf_dict['rerank'] or 'db' not in conf_dict['rerank']['input']:
            if 'documents' in conf_dict and 'db' in conf_dict['documents'] and 'path' in conf_dict['documents']['db']:
                if 'input' not in conf_dict['rerank']:
                    conf_dict['rerank']['input'] = {}
                if 'db' not in conf_dict['rerank']['input']:
                    conf_dict['rerank']['input']['db'] = {'path': conf_dict['documents']['db']['path']}
            else:
                raise ConfigError("documents.db.path needs to be set for this run")

    @staticmethod
    def _update_relative_paths(conf_dict):
        # set path for components to be under the base directory of run
        base = pathlib.Path(conf_dict['path'])
        for c in conf_dict.values():
            if isinstance(c, dict):
                if 'save' in c and not isinstance(c['save'], bool):
                    c['save'] = str(base / c['save'])
        if 'documents' in conf_dict:
            conf_dict['documents']['db']['path'] = str(base / conf_dict['documents']['db']['path'])


class PipelineBuilder:
    def __init__(self, conf):
        self.conf = conf

    def build(self):
        stage1_plan, stage2_plan = self.create_plan(self.conf)
        stage1 = self.build_stage1(self.conf, stage1_plan)
        stage2 = self.build_stage2(self.conf, stage2_plan)
        return stage1, stage2

    def build_stage1(self, conf, plan):
        if not plan:
            return None
        tasks = []
        iterable = None
        if Tasks.DOCUMENTS in plan:
            iterable = DocumentReaderFactory.create(conf.documents.input)
            if not is_complete(conf.documents.db.path) and pathlib.Path(conf.documents.db.path).exists():
                delete_dir(conf.documents.db.path)
            db = DocumentDatabaseFactory.create(conf.documents.db.path)
            tasks.append(DocumentProcessorFactory.create(conf.documents.process, db))
            if conf.documents.save:
                tasks.append(DocWriter(conf.documents.save))
        if Tasks.INDEX in plan:
            # TODO set iterable if documents output exists or throw error if we don't support that
            if pathlib.Path(conf.index.save).exists():
                delete_dir(conf.index.save)
            tasks.append(IndexerFactory.create(conf.index))
        return Pipeline(tasks, iterable)

    def build_stage2(self, conf, plan):
        if not plan:
            return None
        tasks = []
        iterable = None
        if Tasks.TOPICS in plan:
            if pathlib.Path(conf.topics.save).exists():
                delete_dir(conf.topics.save)
            iterable = TopicReaderFactory.create(conf.topics.input)
            tasks.append(TopicProcessorFactory.create(conf.topics.process))
            if conf.topics.save:
                tasks.append(QueryWriter(conf.topics.save))
        if Tasks.RETRIEVE in plan:
            if pathlib.Path(conf.retrieve.save).exists():
                delete_dir(conf.retrieve.save)
            if Tasks.TOPICS not in plan:
                iterable = QueryReader(conf.topics.save)
            tasks.append(RetrieverFactory.create(conf.retrieve))
            if conf.retrieve.save:
                tasks.append(JsonResultsWriter(conf.retrieve.save))
        if Tasks.RERANK in plan:
            if pathlib.Path(conf.rerank.save).exists():
                delete_dir(conf.rerank.save)
            if Tasks.RETRIEVE not in plan:
                iterable = JsonResultsReader(conf.retrieve.save)
            tasks.append(RerankFactory.create(conf.rerank))
            tasks.append(TrecResultsWriter(conf.rerank.save))
        if Tasks.SCORE in plan:
            qrels = QrelsReaderFactory.create(conf.score.input).read()
            tasks.append(Scorer(conf.score, qrels))
        return Pipeline(tasks, iterable)

    def create_plan(self, conf):
        stage1 = []
        if conf.documents:
            # TODO not handling documents complete or validating input
            if not self.is_task_complete(conf.index):
                stage1.append(Tasks.DOCUMENTS)
        if conf.index:
            # TODO not handling checking input
            if not self.is_task_complete(conf.index):
                stage1.append(Tasks.INDEX)

        stage2 = []
        if conf.topics:
            # TODO not handling input check
            if not self.is_task_complete(conf.topics) and not self.is_task_complete(conf.retrieve):
                stage2.append(Tasks.TOPICS)
        if conf.retrieve:
            # TODO not handling input check
            if not self.is_task_complete(conf.retrieve):
                stage2.append(Tasks.RETRIEVE)
        if conf.rerank:
            # TODO not handling input check
            stage2.append(Tasks.RERANK)
        if conf.score:
            if Tasks.RERANK not in stage2 and Tasks.RETRIEVE not in stage2:
                raise ConfigError("Scorer can only run if either retrieve or rerank is configured")
            stage2.append(Tasks.SCORE)

        if not stage1 and not stage2:
            raise ConfigError("No tasks are configured to run")

        return stage1, stage2

    def is_task_complete(self, task_conf):
        return task_conf.save and is_complete(task_conf.save)


class System:
    def __init__(self, config_filename, verbose=False, overrides=None):
        self.setup_logging(verbose)
        conf = RunConfigPreprocessor.process(config_filename, overrides)
        builder = PipelineBuilder(conf)
        self.stage1, self.stage2 = builder.build()

    def run(self):
        if self.stage1:
            LOGGER.info("Stage 1 pipeline: %s", self.stage1)
        if self.stage2:
            LOGGER.info("Stage 2 pipeline: %s", self.stage2)

        if self.stage1:
            LOGGER.info("Starting processing of documents")
            self.stage1.run()
            LOGGER.info("Ingested %s documents", self.stage1.count)

        if self.stage2:
            LOGGER.info("Starting processing of topics")
            self.stage2.run()
            LOGGER.info("Processed %s topics", self.stage2.count)

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
