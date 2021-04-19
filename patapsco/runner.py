import copy
import enum
import json
import logging
import pathlib

from .config import BaseConfig, ConfigService, Optional
from .docs import DocumentsConfig, DocumentProcessorFactory, DocumentReaderFactory, \
    DocumentDatabaseFactory, DocReader, DocWriter
from .error import ConfigError
from .index import IndexConfig, IndexerFactory
from .pipeline import Pipeline
from .rerank import RerankConfig, RerankFactory
from .results import JsonResultsWriter, JsonResultsReader, TrecResultsWriter
from .retrieve import RetrieveConfig, RetrieverFactory
from .score import QrelsReaderFactory, ScoreConfig, Scorer
from .topics import TopicProcessor, TopicReaderFactory, TopicsConfig, QueriesConfig, QueryProcessor, \
    QueryReader, QueryWriter
from .util import Timer
from .util.file import delete_dir, is_complete

LOGGER = logging.getLogger(__name__)


class RunConfig(BaseConfig):
    """Configuration for a run of the system"""
    path: str  # base path for run output
    name: Optional[str]


class RunnerConfig(BaseConfig):
    """Configuration for the patapsco runner"""
    run: RunConfig
    documents: Optional[DocumentsConfig]
    index: Optional[IndexConfig]
    topics: Optional[TopicsConfig]
    queries: Optional[QueriesConfig]
    retrieve: Optional[RetrieveConfig]
    rerank: Optional[RerankConfig]
    score: Optional[ScoreConfig]


class Tasks(enum.Enum):
    """Tasks that make up the system pipelines"""
    DOCUMENTS = enum.auto()
    INDEX = enum.auto()
    TOPICS = enum.auto()
    QUERIES = enum.auto()
    RETRIEVE = enum.auto()
    RERANK = enum.auto()
    SCORE = enum.auto()


class ArtifactConfigPreparer:
    """Prepares the configuration that resulted in an artifact

    This excludes the parts of the configuration that were not used to create the artifact.
    """
    def __init__(self):
        self.contributors = {}
        contributors = ['documents', 'index', 'topics', 'queries', 'retrieve', 'rerank', 'score']
        for task in Tasks:
            contributors.pop(0)
            self.contributors[task] = copy.copy(contributors)

    def get_config(self, config, task):
        return config.copy(exclude=set(self.contributors[task]), deep=True)


class ConfigPreprocessor:
    """Processes the config dictionary before creating the config object with its validation

    1. sets the output directory names from defaults if not already set
    2. sets the paths for output to be under the run directory
    3. sets the retriever's index path based on the index task if not already set
    4. sets the rerankers' db path based on the document processor if not already set
    """

    @classmethod
    def process(cls, config_filename, overrides):
        config_service = ConfigService(overrides)
        try:
            conf_dict = config_service.read_config_file(config_filename)
        except FileNotFoundError as error:
            raise ConfigError(error)
        cls._validate(conf_dict)
        cls._set_output_paths(conf_dict)
        cls._update_relative_paths(conf_dict)
        cls._set_retrieve_input_path(conf_dict)
        cls._set_rerank_db_path(conf_dict)
        return config_service.create_config_object(RunnerConfig, conf_dict)

    @staticmethod
    def _validate(conf_dict):
        # This tests for:
        # 1. The run base path is set
        try:
            conf_dict['run']['path']
        except KeyError:
            raise ConfigError("run.path is not set")

    output_defaults = {
        'documents': False,
        'index': {'path': 'index'},
        'topics': {'path': 'raw_queries'},
        'queries': {'path': 'processed_queries'},
        'retrieve': {'path': 'retrieve'},
        'rerank': {'path': 'rerank'},
        'database': {'path': 'database'}
    }

    @classmethod
    def _set_output_paths(cls, conf_dict):
        # set output path for components from defaults
        for task in cls.output_defaults.keys():
            if task in conf_dict and 'output' not in conf_dict[task]:
                conf_dict[task]['output'] = cls.output_defaults[task]
        if 'documents' in conf_dict and 'db' not in conf_dict['documents']:
            conf_dict['documents']['db'] = cls.output_defaults['database']

    @staticmethod
    def _update_relative_paths(conf_dict):
        # set path for components to be under the base directory of run
        # note that if the path is an absoluate path, pathlib does not change it.
        base = pathlib.Path(conf_dict['run']['path'])
        for c in conf_dict.values():
            if isinstance(c, dict):
                if 'output' in c and not isinstance(c['output'], bool):
                    if 'path' in c['output']:
                        c['output']['path'] = str(base / c['output']['path'])
        if 'documents' in conf_dict:
            try:
                conf_dict['documents']['db']['path'] = str(base / conf_dict['documents']['db']['path'])
            except KeyError:
                raise ConfigError("documents.db.path needs to be set")

    @staticmethod
    def _set_retrieve_input_path(conf_dict):
        # if index location for retrieve is not set, we grab it from index config
        if 'retrieve' in conf_dict:
            if 'input' not in conf_dict['retrieve'] or 'index' not in conf_dict['retrieve']['input']:
                if 'index' in conf_dict and 'output' in conf_dict['index'] and conf_dict['index']['output'] and \
                        'path' in conf_dict['index']['output']:
                    if 'input' not in conf_dict['retrieve']:
                        conf_dict['retrieve']['input'] = {}
                    if 'index' not in conf_dict['retrieve']['input']:
                        conf_dict['retrieve']['input']['index'] = {'path': conf_dict['index']['output']['path']}
                else:
                    raise ConfigError("retrieve.input.index.path needs to be set")

    @staticmethod
    def _set_rerank_db_path(conf_dict):
        # if db path for rerank is not set, we grab it from documents config
        if 'rerank' in conf_dict:
            if 'input' not in conf_dict['rerank'] or 'db' not in conf_dict['rerank']['input']:
                if 'documents' in conf_dict and 'db' in conf_dict['documents'] and 'path' in conf_dict['documents']['db']:
                    if 'input' not in conf_dict['rerank']:
                        conf_dict['rerank']['input'] = {}
                    if 'db' not in conf_dict['rerank']['input']:
                        conf_dict['rerank']['input']['db'] = {'path': conf_dict['documents']['db']['path']}
                else:
                    raise ConfigError("rerank.input.db.path needs to be set")


class PipelineBuilder:
    """Builds the stage 1 and stage 2 pipelines

    Analyzes the configuration to create a plan which tasks to include.
    Then builds the pipelines based on the plan and configuration.
    Handles restarting a run where it left off.
    Will create pipelines for partial runs (that end early or start from artifacts).
    """
    def __init__(self, conf):
        self.conf = conf
        self.artifact_tool = ArtifactConfigPreparer()

    def build(self):
        stage1_plan, stage2_plan = self.create_plan(self.conf)
        stage1 = self.build_stage1(stage1_plan)
        stage2 = self.build_stage2(stage2_plan)
        return stage1, stage2

    def create_plan(self, conf):
        # Analyze the config and check there are any artifacts from a previous run.
        # A plan consists of a list of Tasks to be constructed into a pipeline.
        stage1 = []
        if conf.documents:
            index_complete = conf.index and self.is_task_complete(conf.index)
            if not self.is_task_complete(conf.documents) and not index_complete:
                stage1.append(Tasks.DOCUMENTS)
        if conf.index:
            if not self.is_task_complete(conf.index):
                stage1.append(Tasks.INDEX)

        stage2 = []
        # TODO need to confirm that the db is also built
        retrieve_complete = conf.retrieve and self.is_task_complete(conf.retrieve)
        if conf.topics:
            # add topics task if it is not complete, the queries are not available and the retrieve task is not complete
            if not self.is_task_complete(conf.topics) and not self.is_task_complete(conf.queries) and \
                    not retrieve_complete:
                stage2.append(Tasks.TOPICS)
        if conf.queries:
            if not self.is_task_complete(conf.queries) and not retrieve_complete:
                stage2.append(Tasks.QUERIES)
        if conf.retrieve:
            if not self.is_task_complete(conf.retrieve):
                stage2.append(Tasks.RETRIEVE)
        if conf.rerank:
            if self.is_task_complete(conf.rerank):
                raise ConfigError('Rerank is already complete. Delete its output directory to rerun reranking.')
            stage2.append(Tasks.RERANK)
        if conf.score:
            if Tasks.RERANK not in stage2 and Tasks.RETRIEVE not in stage2:
                raise ConfigError("Scorer can only run if either retrieve or rerank is configured")
            stage2.append(Tasks.SCORE)

        if not stage1 and not stage2:
            raise ConfigError("No tasks are configured to run")

        return stage1, stage2

    def build_stage1(self, plan):
        # Stage 1 is generally: read docs, process them, build index.
        # For each task, we clear previous data from a failed run if it exists.
        # Then we build the tasks from the plan and create the iterator to drive the pipeline.
        if not plan:
            return None
        tasks = []
        iterable = None
        if Tasks.DOCUMENTS in plan:
            # doc reader -> doc processor with doc db -> optional doc writer
            self.clear_output(self.conf.documents)
            artifact_conf = self.artifact_tool.get_config(self.conf, Tasks.DOCUMENTS)
            if not is_complete(self.conf.documents.db.path) and pathlib.Path(self.conf.documents.db.path).exists():
                delete_dir(self.conf.documents.db.path)
            iterable = DocumentReaderFactory.create(self.conf.documents.input)
            db = DocumentDatabaseFactory.create(self.conf.documents.db.path, artifact_conf)
            tasks.append(DocumentProcessorFactory.create(self.conf.documents.process, db))
            if self.conf.documents.output and self.conf.documents.output.path:
                tasks.append(DocWriter(self.conf.documents.output.path, artifact_conf))

        if Tasks.INDEX in plan:
            self.clear_output(self.conf.index)
            artifact_conf = self.artifact_tool.get_config(self.conf, Tasks.INDEX)
            if Tasks.DOCUMENTS not in plan:
                # documents already processed so locate them to set the iterator
                iterable = self._setup_input(DocReader, 'index.input.documents.path',
                                            'documents.output.path', 'index not configured with documents')
            tasks.append(IndexerFactory.create(self.conf.index, artifact_conf))
        return Pipeline(tasks, iterable)

    def build_stage2(self, plan):
        # Stage 2 is generally: read topics, extract query, process them, retrieve results, rerank them, score.
        # For each task, we clear previous data from a failed run if it exists.
        # Then we build the tasks from the plan and create the iterator to drive the pipeline.
        if not plan:
            return None
        tasks = []
        iterable = None
        if Tasks.TOPICS in plan:
            # topic reader -> topic processor -> optional query writer
            self.clear_output(self.conf.topics)
            artifact_conf = self.artifact_tool.get_config(self.conf, Tasks.TOPICS)
            iterable = TopicReaderFactory.create(self.conf.topics.input)
            tasks.append(TopicProcessor(self.conf.topics))
            if self.conf.topics.output:
                tasks.append(QueryWriter(self.conf.topics.output.path, artifact_conf))

        if Tasks.QUERIES in plan:
            # optional query reader -> query processor -> optional query writer
            self.clear_output(self.conf.queries)
            artifact_conf = self.artifact_tool.get_config(self.conf, Tasks.QUERIES)
            if Tasks.TOPICS not in plan:
                iterable = self._setup_input(QueryReader, 'queries.input.path', 'topics.output.path',
                                             'query processor not configured with input')
            tasks.append(QueryProcessor(self.conf.queries.process))
            if self.conf.queries.output:
                tasks.append(QueryWriter(self.conf.queries.output.path, artifact_conf))

        if Tasks.RETRIEVE in plan:
            self.clear_output(self.conf.retrieve)
            artifact_conf = self.artifact_tool.get_config(self.conf, Tasks.RETRIEVE)
            if Tasks.QUERIES not in plan:
                # queries already processed so locate them to set the iterator
                iterable = self._setup_input(QueryReader, 'retrieve.input.queries.path', 'queries.output.path',
                                             'retrieve not configured with queries')
            tasks.append(RetrieverFactory.create(self.conf.retrieve))
            if self.conf.retrieve.output:
                tasks.append(JsonResultsWriter(self.conf.retrieve.output.path, artifact_conf))

        if Tasks.RERANK in plan:
            self.clear_output(self.conf.rerank)
            artifact_conf = self.artifact_tool.get_config(self.conf, Tasks.RERANK)
            if Tasks.RETRIEVE not in plan:
                # retrieve results already processed so locate them to set the iterator
                iterable = self._setup_input(JsonResultsReader, 'rerank.input.results.path', 'retrieve.output.path',
                                             'rerank not configured with retrieve results')
            db = DocumentDatabaseFactory.create(self.conf.rerank.input.db.path)
            tasks.append(RerankFactory.create(self.conf.rerank, db))
            tasks.append(TrecResultsWriter(self.conf.rerank.output.path, artifact_conf))

        if Tasks.SCORE in plan:
            qrels = QrelsReaderFactory.create(self.conf.score.input).read()
            tasks.append(Scorer(self.conf.score, qrels))
        return Pipeline(tasks, iterable)

    def _setup_input(self, cls, path1, path2, error_msg):
        """Try two possible places for input path"""
        obj = self.conf
        fields = path1.split('.')
        try:
            while fields:
                field = fields.pop(0)
                obj = getattr(obj, field)
            return cls(obj)
        except AttributeError:
            obj = self.conf
            fields = path2.split('.')
            try:
                while fields:
                    field = fields.pop(0)
                    obj = getattr(obj, field)
                return cls(obj)
            except AttributeError:
                raise ConfigError(error_msg)

    @staticmethod
    def is_task_complete(task_conf):
        if task_conf is None:
            return False
        return task_conf.output and is_complete(task_conf.output.path)

    @staticmethod
    def clear_output(task_conf):
        if task_conf.output and pathlib.Path(task_conf.output.path).exists():
            delete_dir(task_conf.output.path)


class Runner:
    def __init__(self, config_filename, verbose=False, overrides=None):
        self.setup_logging(verbose)
        self.conf = ConfigPreprocessor.process(config_filename, overrides)
        builder = PipelineBuilder(self.conf)
        self.stage1, self.stage2 = builder.build()

    def run(self):
        if self.conf.run.name:
            LOGGER.info("Starting run: %s", self.conf.run.name)
        if self.stage1:
            LOGGER.info("Stage 1 pipeline: %s", self.stage1)
        if self.stage2:
            LOGGER.info("Stage 2 pipeline: %s", self.stage2)

        if self.stage1:
            timer1 = Timer()
            LOGGER.info("Stage 1: Starting processing of documents")
            with timer1:
                self.stage1.run()
            LOGGER.info("Stage 1: Ingested %d documents", self.stage1.count)
            LOGGER.info("Stage 1 took %.1f secs", timer1.time)

        if self.stage2:
            timer2 = Timer()
            LOGGER.info("Stage 2: Starting processing of topics")
            with timer2:
                self.stage2.run()
            LOGGER.info("Stage 2: Processed %d topics", self.stage2.count)
            LOGGER.info("Stage 2 took %.1f secs", timer2.time)

        self.write_config()
        self.write_report()
        LOGGER.info("Run complete")

    @staticmethod
    def setup_logging(verbose):
        log_level = logging.DEBUG if verbose else logging.INFO
        logger = logging.getLogger('patapsco')
        logger.setLevel(log_level)
        console = logging.StreamHandler()
        console.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console.setFormatter(formatter)
        logger.addHandler(console)

    def write_report(self):
        # TODO maybe rename this as timing.txt
        path = pathlib.Path(self.conf.run.path) / 'report.txt'
        data = {}
        if self.stage1:
            data['stage1'] = self.stage1.report
        if self.stage2:
            data['stage2'] = self.stage2.report
        with open(path, 'w') as fp:
            json.dump(data, fp, indent=4)

    def write_config(self):
        path = pathlib.Path(self.conf.run.path) / 'config.yml'
        ConfigService.write_config_file(str(path), self.conf)
