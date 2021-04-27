import enum
import functools
import json
import logging
import pathlib

from .config import ConfigService
from .docs import DocumentProcessor, DocumentReaderFactory, DocumentDatabaseFactory, DocReader, DocWriter
from .error import ConfigError
from .index import IndexerFactory
from .pipeline import BatchPipeline, MultiplexTask, StreamingPipeline
from .rerank import RerankFactory
from .results import JsonResultsWriter, JsonResultsReader, TrecResultsWriter
from .retrieve import Joiner, RetrieverFactory
from .schema import RunnerConfig
from .score import QrelsReaderFactory, Scorer
from .topics import TopicProcessor, TopicReaderFactory, QueryProcessor, QueryReader, QueryWriter
from .util import SlicedIterator, Timer
from .util.file import delete_dir, is_complete

LOGGER = logging.getLogger(__name__)


class PipelineMode(str, enum.Enum):
    STREAMING = 'streaming'
    BATCH = 'batch'


class Tasks(enum.Enum):
    """Tasks that make up the system pipelines"""
    DOCUMENTS = enum.auto()
    INDEX = enum.auto()
    TOPICS = enum.auto()
    QUERIES = enum.auto()
    RETRIEVE = enum.auto()
    RERANK = enum.auto()
    SCORE = enum.auto()


class Job:
    def __init__(self, conf, stage1, stage2):
        self.conf = conf
        self.run_path = conf.run.path
        self.stage1 = stage1
        self.stage2 = stage2

    def run(self):
        if self.conf.run.name:
            LOGGER.info("Starting run: %s", self.conf.run.name)

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

    def write_report(self):
        path = pathlib.Path(self.run_path) / 'timing.txt'
        data = {}
        if self.stage1:
            data['stage1'] = self.stage1.report
        if self.stage2:
            data['stage2'] = self.stage2.report
        with open(path, 'w') as fp:
            json.dump(data, fp, indent=4)

    def write_config(self):
        path = pathlib.Path(self.run_path) / 'config.yml'
        ConfigService.write_config_file(str(path), self.conf)


class ArtifactHelper:
    """Utilities for working with artifacts"""

    TASKS = ['documents', 'index', 'topics', 'queries', 'retrieve', 'rerank', 'score']

    def __init__(self):
        self.contributors = {}
        contributors = list(self.TASKS)
        for task in Tasks:
            contributors.pop(0)
            self.contributors[task] = list(contributors)

    def get_config(self, config, task):
        """This excludes the parts of the configuration that were not used to create the artifact."""
        return config.copy(exclude=set(self.contributors[task]), deep=True)

    def combine(self, config, path):
        """Loads an artifact configuration and combines it with the base config"""
        path = pathlib.Path(path)
        if path.is_dir():
            path = path / 'config.yml'
        else:
            path = path.parent / 'config.yml'
        try:
            artifact_config_dict = ConfigService().read_config_file(path)
        except FileNotFoundError:
            raise ConfigError(f"Unable to load artifact config {path}")
        artifact_config = RunnerConfig(**artifact_config_dict)
        for task in self.TASKS:
            if getattr(artifact_config, task):
                if not getattr(config, task):
                    setattr(config, task, getattr(artifact_config, task))


class ConfigPreprocessor:
    """Processes the config dictionary before creating the config object with its validation

    1. sets the run output if not set based on run name
    2. sets the output directory names from defaults if not already set
    3. sets the paths for output to be under the run output directory
    4. sets the retriever's index path based on the index task if not already set
    5. sets the reranker's db path based on the document processor if not already set
    """

    @classmethod
    def process(cls, config_filename, overrides):
        config_service = ConfigService(overrides)
        try:
            conf_dict = config_service.read_config_file(config_filename)
        except FileNotFoundError as error:
            raise ConfigError(error)
        cls._validate(conf_dict)
        cls._set_run_path(conf_dict)
        cls._set_output_paths(conf_dict)
        cls._update_relative_paths(conf_dict)
        cls._set_retrieve_input_path(conf_dict)
        cls._set_rerank_db_path(conf_dict)
        return config_service.create_config_object(RunnerConfig, conf_dict)

    @staticmethod
    def _validate(conf_dict):
        # This tests for:
        #  1. The run name is set
        try:
            conf_dict['run']['name']
        except KeyError:
            raise ConfigError("run.name is not set")

    @classmethod
    def _set_run_path(cls, conf_dict):
        # set run path from name if not already set
        if 'path' not in conf_dict['run']:
            mapping = str.maketrans(" ", "-", "'\",")
            path = conf_dict['run']['name'].translate(mapping)
            conf_dict['run']['path'] = str(pathlib.Path('runs') / path)

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
        # note that if the path is an absolute path, pathlib does not change it.
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


class JobBuilder:
    """Builds a Job based on stage 1 and stage 2 pipelines

    Analyzes the configuration to create a plan of which tasks to include.
    Then builds the pipelines based on the plan and configuration.
    Handles restarting a run where it left off.
    Will create pipelines for partial runs (that end early or start from artifacts).
    """
    def __init__(self, conf):
        """
        Args:
            conf (RunnerConfig): Configuration for the runner.
        """
        self.conf = conf
        self.artifact_helper = ArtifactHelper()
        self.doc_lang = None
        self.query_lang = None

    def build(self):
        stage1 = stage2 = None

        stage1_plan = self._create_stage1_plan()
        if stage1_plan:
            stage1_iter = self._get_stage1_iterator(stage1_plan)
            stage1_tasks = self._get_stage1_tasks(stage1_plan)
            stage1 = self._build_stage1_pipeline(stage1_iter, stage1_tasks)

        stage2_plan = self._create_stage2_plan()
        if stage2_plan:
            stage2_iter = self._get_stage2_iterator(stage2_plan)
            stage2_tasks = self._get_stage2_tasks(stage2_plan)
            stage2 = self._build_stage2_pipeline(stage2_iter, stage2_tasks)

        if not stage1 and not stage2:
            raise ConfigError("No tasks are configured to run")

        if not stage1 and stage2 and Tasks.RERANK in stage2_plan:
            self.check_sources_of_documents()
        if stage2 and Tasks.RETRIEVE in stage2_plan:
            self.check_text_processing()

        return Job(self.conf, stage1, stage2)

    def _create_stage1_plan(self):
        # Analyze the config and check there are any artifacts from a previous run.
        # A plan consists of a list of Tasks to be constructed into a pipeline.
        stage1 = []
        index_complete = self.conf.index and self.is_task_complete(self.conf.index)
        if self.conf.documents:
            if not self.is_task_complete(self.conf.documents) and not index_complete:
                stage1.append(Tasks.DOCUMENTS)
        if self.conf.index:
            if not index_complete:
                stage1.append(Tasks.INDEX)
        return stage1

    def _get_stage1_iterator(self, plan):
        # Get the iterator for pipeline based on plan and configuration
        if Tasks.DOCUMENTS in plan:
            iterator = DocumentReaderFactory.create(self.conf.documents.input)
        else:
            # documents already processed so locate them to create the iterator and update config
            iterator = self._setup_input(DocReader, 'index.input.documents.path',
                                         'documents.output.path', 'index not configured with documents')
        stage_conf = self.conf.run.stage1
        return SlicedIterator(iterator, stage_conf.start, stage_conf.stop)

    def _get_stage1_tasks(self, plan):
        # Stage 1 is generally: read docs, process them, build index.
        # For each task, we clear previous data from a failed run if it exists.
        # Then we build the tasks from the plan and configuration.
        tasks = []

        if Tasks.DOCUMENTS in plan:
            # doc reader -> doc processor with doc db -> optional doc writer
            self.docs_lang = self.standardize_language(self.conf.documents.input)
            self.clear_output(self.conf.documents, clear_db=True)
            artifact_conf = self.artifact_helper.get_config(self.conf, Tasks.DOCUMENTS)
            db = DocumentDatabaseFactory.create(self.conf.documents.db.path, artifact_conf)
            tasks.append(DocumentProcessor(self.conf.documents.process, self.docs_lang, db))
            # add doc writer if user requesting that we save processed docs
            if self.conf.documents.output and self.conf.documents.output.path:
                if self.conf.documents.process.splits:
                    # if we are splitting the documents output, multiplex the doc writer
                    tasks.append(MultiplexTask(self.conf.documents.process.splits, DocWriter,
                                               self.conf.documents, artifact_conf))
                else:
                    tasks.append(DocWriter(self.conf.documents, artifact_conf))

        if Tasks.INDEX in plan:
            # indexer or processed doc reader -> indexer
            self.clear_output(self.conf.index)
            artifact_conf = self.artifact_helper.get_config(self.conf, Tasks.INDEX)
            if self.conf.documents.process.splits:
                # if we are splitting the documents output, multiplex the indexer
                tasks.append(MultiplexTask(self.conf.documents.process.splits, IndexerFactory.create,
                                           self.conf.index, artifact_conf))
            else:
                tasks.append(IndexerFactory.create(self.conf.index, artifact_conf))

        return tasks

    def _build_stage1_pipeline(self, iterator, tasks):
        # select pipeline based on stage configuration
        stage_conf = self.conf.run.stage1
        if stage_conf.mode == PipelineMode.STREAMING:
            LOGGER.info("Stage 1 is a streaming pipeline")
            pipeline_class = StreamingPipeline
        elif stage_conf.mode == PipelineMode.BATCH:
            batch_size_char = str(stage_conf.batch_size) if stage_conf.batch_size else '∞'
            LOGGER.info("Stage 1 is a batch pipeline selected with batch size of %s", batch_size_char)
            pipeline_class = functools.partial(BatchPipeline, n=stage_conf.batch_size)
        else:
            raise ConfigError(f"Unrecognized pipeline mode: {stage_conf.mode}")
        pipeline = pipeline_class(iterator, tasks)
        LOGGER.info("Stage 1 pipeline: %s", pipeline)
        return pipeline

    def _create_stage2_plan(self):
        # Analyze the config and check there are any artifacts from a previous run.
        # A plan consists of a list of Tasks to be constructed into a pipeline.
        stage2 = []
        # TODO need to confirm that the db is also built
        retrieve_complete = self.conf.retrieve and self.is_task_complete(self.conf.retrieve)
        if self.conf.topics:
            # add topics task if it is not complete, the queries are not available and the retrieve task is not complete
            if not self.is_task_complete(self.conf.topics) and not self.is_task_complete(self.conf.queries) and \
                    not retrieve_complete:
                stage2.append(Tasks.TOPICS)
        if self.conf.queries:
            if not self.is_task_complete(self.conf.queries) and not retrieve_complete:
                stage2.append(Tasks.QUERIES)
        if self.conf.retrieve:
            if not self.is_task_complete(self.conf.retrieve):
                stage2.append(Tasks.RETRIEVE)
        if self.conf.rerank:
            if self.is_task_complete(self.conf.rerank):
                raise ConfigError('Rerank is already complete. Delete its output directory to rerun reranking.')
            stage2.append(Tasks.RERANK)
        if self.conf.score:
            if Tasks.RERANK not in stage2 and Tasks.RETRIEVE not in stage2:
                raise ConfigError("Scorer can only run if either retrieve or rerank is configured.")
            stage2.append(Tasks.SCORE)
        return stage2

    def _get_stage2_iterator(self, plan):
        # Get the iterator for pipeline based on plan and configuration
        if not plan:
            return None
        if Tasks.TOPICS in plan:
            iterator = TopicReaderFactory.create(self.conf.topics.input)
        elif Tasks.QUERIES in plan:
            iterator = self._setup_input(QueryReader, 'queries.input.path', 'topics.output.path',
                                         'query processor not configured with input')
            query = iterator.peek()
            self.query_lang = query.lang
        elif Tasks.RETRIEVE in plan:
            iterator = self._setup_input(QueryReader, 'retrieve.input.queries.path', 'queries.output.path',
                                         'retrieve not configured with queries')
        else:
            iterator = self._setup_input(JsonResultsReader, 'rerank.input.results.path', 'retrieve.output.path',
                                         'rerank not configured with retrieve results')
        stage_conf = self.conf.run.stage2
        return SlicedIterator(iterator, stage_conf.start, stage_conf.stop)

    def _get_stage2_tasks(self, plan):
        # Stage 2 is generally: read topics, extract query, process them, retrieve results, rerank them, score.
        # For each task, we clear previous data from a failed run if it exists.
        # Then we build the tasks from the plan and configuration.
        tasks = []

        if Tasks.TOPICS in plan:
            # topic reader -> topic processor -> optional query writer
            self.query_lang = self.standardize_language(self.conf.topics.input)
            self.clear_output(self.conf.topics)
            artifact_conf = self.artifact_helper.get_config(self.conf, Tasks.TOPICS)
            tasks.append(TopicProcessor(self.conf.topics))
            if self.conf.topics.output:
                tasks.append(QueryWriter(self.conf.topics, artifact_conf))

        if Tasks.QUERIES in plan:
            # optional query reader -> query processor -> optional query writer
            self.clear_output(self.conf.queries)
            tasks.append(QueryProcessor(self.conf.queries.process, self.query_lang))
            artifact_conf = self.artifact_helper.get_config(self.conf, Tasks.QUERIES)
            if self.conf.queries.output:
                if self.conf.queries.process.splits:
                    tasks.append(MultiplexTask(self.conf.queries.process.splits, QueryWriter,
                                               self.conf.queries, artifact_conf))
                else:
                    tasks.append(QueryWriter(self.conf.queries, artifact_conf))

        if Tasks.RETRIEVE in plan:
            self.clear_output(self.conf.retrieve)
            if not self.conf.index:
                # copy in the configuration that created the index (this path is always set in the ConfigPreprocessor)
                self.artifact_helper.combine(self.conf, self.conf.retrieve.input.index.path)
            artifact_conf = self.artifact_helper.get_config(self.conf, Tasks.RETRIEVE)
            tasks.append(RetrieverFactory.create(self.conf.retrieve))
            if self.conf.queries.process.splits:
                tasks.append(Joiner())
            if self.conf.retrieve.output:
                tasks.append(JsonResultsWriter(self.conf.retrieve, artifact_conf))

        if Tasks.RERANK in plan:
            self.clear_output(self.conf.rerank)
            artifact_conf = self.artifact_helper.get_config(self.conf, Tasks.RERANK)
            db = DocumentDatabaseFactory.create(self.conf.rerank.input.db.path, readonly=True)
            tasks.append(RerankFactory.create(self.conf.rerank, db))
            tasks.append(TrecResultsWriter(self.conf.rerank, artifact_conf))

        if Tasks.SCORE in plan:
            qrels = QrelsReaderFactory.create(self.conf.score.input).read()
            tasks.append(Scorer(self.conf.score, qrels))

        return tasks

    def _build_stage2_pipeline(self, iterator, tasks):
        # select pipeline based on stage configuration
        stage_conf = self.conf.run.stage2
        if stage_conf.mode == PipelineMode.STREAMING:
            LOGGER.info("Stage 2 is a streaming pipeline")
            pipeline_class = StreamingPipeline
        elif stage_conf.mode == PipelineMode.BATCH:
            batch_size_char = str(stage_conf.batch_size) if stage_conf.batch_size else '∞'
            LOGGER.info("Stage 2 is a batch pipeline selected with batch size of %s", batch_size_char)
            pipeline_class = functools.partial(BatchPipeline, n=stage_conf.batch_size)
        else:
            raise ConfigError(f"Unrecognized pipeline mode: {stage_conf.mode}")
        pipeline = pipeline_class(iterator, tasks)
        LOGGER.info("Stage 2 pipeline: %s", pipeline)
        return pipeline

    def _setup_input(self, cls, input_path, output_path, error_msg):
        """Try two possible places for input path

        The input for this task could come from:
          1. the configured input of this task
          2. the configured output of the previous task

        This also loads the configuration from the input directory and puts it into the main config.

        Raises:
            ConfigError if neither path is configured
        """
        obj = self.conf
        fields = input_path.split('.')
        try:
            while fields:
                field = fields.pop(0)
                obj = getattr(obj, field)
            self.artifact_helper.combine(self.conf, obj)
            return cls(obj)
        except AttributeError:
            obj = self.conf
            fields = output_path.split('.')
            try:
                while fields:
                    field = fields.pop(0)
                    obj = getattr(obj, field)
                self.artifact_helper.combine(self.conf, obj)
                return cls(obj)
            except AttributeError:
                raise ConfigError(error_msg)

    @staticmethod
    def is_task_complete(task_conf):
        """Checks whether the task is already complete"""
        if task_conf is None:
            return False
        return task_conf.output and is_complete(task_conf.output.path)

    @staticmethod
    def clear_output(task_conf, clear_db=False):
        """Delete the output directory if previous run did not complete

        Args:
            task_conf (BaseConfig): Configuration for a task.
            clear_db (bool): Whether to also clear the database.
        """
        if task_conf.output and pathlib.Path(task_conf.output.path).exists():
            delete_dir(task_conf.output.path)
        if clear_db and not is_complete(task_conf.db.path) and pathlib.Path(task_conf.db.path).exists():
            delete_dir(task_conf.db.path)

    @staticmethod
    def standardize_language(input_config):
        # using ISO 639
        langs = {
            'ar': 'ar',
            'ara': 'ar',
            'arb': 'ar',
            'en': 'en',
            'eng': 'eng',
            'fa': 'fa',
            'fas': 'fa',
            'per': 'fa',
            'ru': 'ru',
            'rus': 'ru',
            'zh': 'zh',
            'chi': 'zh',
            'zho': 'zh'
        }
        try:
            lang = langs[input_config.lang.lower()]
            input_config.lang = lang
            return lang
        except KeyError:
            raise ConfigError(f"Unknown language code: {input_config.lang}")

    def check_sources_of_documents(self):
        config_path = pathlib.Path(self.conf.rerank.input.db.path) / 'config.yml'
        try:
            artifact_config_dict = ConfigService().read_config_file(config_path)
        except FileNotFoundError:
            LOGGER.warning("Unable to load config for the document database")
            return
        artifact_config = RunnerConfig(**artifact_config_dict)
        if not isinstance(self.conf.documents.input.path, type(artifact_config.documents.input.path)):
            raise ConfigError("documents in index do not match documents in database")
        if isinstance(self.conf.documents.input.path, str):
            name1 = pathlib.Path(self.conf.documents.input.path).name
            name2 = pathlib.Path(artifact_config.documents.input.path).name
            if name1 != name2:
                raise ConfigError("documents in index do not match documents in database")
        elif isinstance(self.conf.documents.input.path, list):
            for p1, p2 in zip(self.conf.documents.input.path, artifact_config.documents.input.path):
                name1 = pathlib.Path(p1).name
                name2 = pathlib.Path(p2).name
                if name1 != name2:
                    raise ConfigError("documents in index do not match documents in database")

    def check_text_processing(self):
        doc = self.conf.documents.process
        query = self.conf.queries.process
        try:
            assert doc.normalize == query.normalize
            assert doc.tokenize == query.tokenize
            assert doc.stopwords == query.stopwords
            assert doc.lowercase == query.lowercase
            assert doc.stem == query.stem
        except AssertionError:
            raise ConfigError("Text processing for documents and queries does not match")
