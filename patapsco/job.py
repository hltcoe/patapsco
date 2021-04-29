import concurrent.futures
import dataclasses
import functools
import json
import logging
import math
import pathlib

from .config import ConfigService
from .docs import DocumentProcessor, DocumentReaderFactory, DocumentDatabaseFactory, DocReader, DocWriter
from .error import ConfigError
from .helpers import ArtifactHelper
from .index import IndexerFactory
from .pipeline import BatchPipeline, MultiplexTask, StreamingPipeline
from .rerank import RerankFactory
from .results import JsonResultsWriter, JsonResultsReader, TrecResultsWriter
from .retrieve import Joiner, RetrieverFactory
from .schema import RunnerConfig, PipelineMode, Tasks
from .score import QrelsReaderFactory, Scorer
from .topics import TopicProcessor, TopicReaderFactory, QueryProcessor, QueryReader, QueryWriter
from .util import SlicedIterator, Timer
from .util.file import delete_dir, is_complete, path_append

LOGGER = logging.getLogger(__name__)


class Job:
    """A job is an executable component of a system run

    For a serial run, a job = the run.
    For a parallel run, the input is divided into chunks and each chunk is a job.
    """

    def __init__(self, conf, stage1, stage2):
        self.conf = conf
        self.run_path = conf.run.path
        self.stage1 = stage1
        self.stage2 = stage2

    def run(self):
        if self.conf.run.name:
            LOGGER.info("Starting run: %s", self.conf.run.name)

        self._run()

        self.write_config()
        # self.write_report()
        LOGGER.info("Run complete")

    def _run(self):
        # Children of Job must implement this which is called by run()
        pass

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


class SerialJob(Job):
    """Single threaded job"""

    def _run(self):
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


@dataclasses.dataclass
class ParallelJobDef:
    id: int
    conf: RunnerConfig


class ParallelJob(Job):
    """Parallel job that uses multiple processes.

    This uses concurrent.futures to implement map/reduce over the input iterators.
    """
    def __init__(self, conf, stage1, stage2):
        super().__init__(conf, stage1, stage2)
        self.num_processes = 2
        self.stage1_jobs = self.stage2_jobs = None
        if stage1:
            self.stage1_jobs = self._get_stage1_jobs()
        if stage2:
            self.stage2_jobs = self._get_stage2_jobs()

    def _run(self):
        if self.stage1_jobs:
            LOGGER.info("Stage 1: Starting processing of documents")
            self.stage1.begin()
            self.map(self.stage1_jobs)
            self.stage1.reduce()
            self.stage1.end()
            LOGGER.info("Stage 1: Ingested %d documents", 7)

        if self.stage2_jobs:
            LOGGER.info("Stage 2: Starting processing of queries")
            self.stage2.begin()
            self.map(self.stage2_jobs)
            self.stage2.reduce()
            self.stage2.end()
            LOGGER.info("Stage 2: Processed %d queries", 7)

    def map(self, jobs):
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.num_processes) as executor:
            # we loop in a try/except to catch errors from the jon running in separate process
            try:
                for _ in executor.map(self._fork, jobs):
                    pass
            except Exception as e:
                LOGGER.error(f"Parallel job failed with {e}")

    @staticmethod
    def _fork(job):
        # only log parallel jobs to their unique log file
        log_file = path_append(job.conf.run.path, f"patapsco.log.{job.id}")
        logger = logging.getLogger('patapsco')
        file = logging.FileHandler(log_file)
        file.setLevel(logger.level)
        file.setFormatter(logger.handlers[0].formatter)
        logger.handlers = []
        logger.addHandler(file)

        job = JobBuilder(job.conf).build()
        job.run()

    def _get_stage1_jobs(self):
        num_items = len(self.stage1.iterator)
        job_size = int(math.ceil(num_items / self.num_processes))
        indices = [(i, i + job_size) for i in range(0, num_items, job_size)]
        stage1_jobs = []
        for part, (start, stop) in enumerate(indices):
            sub_directory = f"part_{part}"
            conf = self.conf.copy(deep=True)
            conf.run.stage1.start = start
            conf.run.stage1.stop = stop
            conf.run.parallel = None
            conf.run.stage2 = False
            self._update_stage1_output_paths(conf, sub_directory)
            stage1_jobs.append(ParallelJobDef(part, conf))
        return stage1_jobs

    def _get_stage2_jobs(self):
        num_items = len(self.stage2.iterator)
        job_size = int(math.ceil(num_items / self.num_processes))
        indices = [(i, i + job_size) for i in range(0, num_items, job_size)]
        stage2_jobs = []
        for part, (start, stop) in enumerate(indices):
            sub_directory = f"part_{part}"
            conf = self.conf.copy(deep=True)
            conf.run.stage2.start = start
            conf.run.stage2.stop = stop
            conf.run.parallel = None
            conf.run.stage1 = False
            self._update_stage2_output_paths(conf, sub_directory)
            stage2_jobs.append(ParallelJobDef(part, conf))
        return stage2_jobs

    @staticmethod
    def _update_stage1_output_paths(conf, part):
        # configs may not have all tasks so we ignore errors
        try:
            conf.documents.db.path = path_append(conf.documents.db.path, part)
        except AttributeError:
            pass
        try:
            conf.documents.output.path = path_append(conf.documents.output.path, part)
        except AttributeError:
            pass
        try:
            conf.index.output.path = path_append(conf.index.output.path, part)
        except AttributeError:
            pass

    @staticmethod
    def _update_stage2_output_paths(conf, part):
        # configs may not have all tasks so we ignore errors
        try:
            conf.topics.output.path = path_append(conf.topics.output.path, part)
        except AttributeError:
            pass
        try:
            conf.queries.output.path = path_append(conf.queries.output.path, part)
        except AttributeError:
            pass
        try:
            conf.retrieve.output.path = path_append(conf.retrieve.output.path, part)
        except AttributeError:
            pass
        try:
            conf.rerank.output.path = path_append(conf.rerank.output.path, part)
        except AttributeError:
            pass


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
        stage1_plan = []
        stage2_plan = []

        if self.conf.run.stage1:
            stage1_plan = self._create_stage1_plan()
            if stage1_plan:
                stage1_iter = self._get_stage1_iterator(stage1_plan)
                stage1_tasks = self._get_stage1_tasks(stage1_plan)
                stage1 = self._build_stage1_pipeline(stage1_iter, stage1_tasks)

        if self.conf.run.stage2:
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

        if self.conf.run.parallel:
            return ParallelJob(self.conf, stage1, stage2)
        else:
            return SerialJob(self.conf, stage1, stage2)

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
