import concurrent.futures
import dataclasses
import functools
import json
import logging
import math
import multiprocessing
import pathlib
import psutil
import sys
import subprocess

from .config import ConfigService
from .docs import DocumentProcessor, DocumentReaderFactory, DocumentDatabaseFactory, DocReader, DocWriter
from .error import ConfigError
from .helpers import ArtifactHelper
from .index import IndexerFactory
from .pipeline import BatchPipeline, StreamingPipeline
from .rerank import RerankFactory
from .results import JsonResultsWriter, JsonResultsReader, TrecResultsWriter
from .retrieve import RetrieverFactory
from .schema import RunnerConfig, PipelineMode, Tasks
from .score import Scorer
from .topics import TopicProcessor, TopicReaderFactory, QueryProcessor, QueryReader, QueryWriter
from .util import DataclassJSONEncoder, get_human_readable_size, LangStandardizer, LoggingFilter, SlicedIterator, Timer
from .util.file import delete_dir, is_complete, path_append

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class StageReport:
    count: int = 0
    timing: list = dataclasses.field(default_factory=list)

    def __add__(self, other):
        if self.timing and not other.timing:
            timing = self.timing
        elif not self.timing and other.timing:
            timing = other.timing
        else:
            timing = [(a[0], a[1] + b[1]) for a, b in zip(self.timing, other.timing)]
        return StageReport(self.count + other.count, timing)


@dataclasses.dataclass
class Report:
    stage1: StageReport = StageReport()
    stage2: StageReport = StageReport()

    def __add__(self, other):
        stage1 = self.stage1 + other.stage1
        stage2 = self.stage2 + other.stage2
        return Report(stage1, stage2)

    def __radd__(self, other):
        if other == 0:
            return self
        else:
            return self.__add__(other)


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

    def run(self, sub_job=False):
        LOGGER.info("Starting run: %s", self.conf.run.name)

        report = self._run()

        if not sub_job:
            self.write_config()
            self.write_report(report)
            self.write_scores()
        mem = psutil.Process().memory_info().rss
        LOGGER.info(f"Memory usage: {get_human_readable_size(mem)}")
        LOGGER.info("Run complete")
        return report

    def _run(self):
        # Children of Job must implement this which is called by run()
        pass

    def write_report(self, report):
        path = pathlib.Path(self.run_path) / 'timing.json'
        with open(path, 'w') as fp:
            json.dump(report, fp, indent=4, cls=DataclassJSONEncoder)

    def write_config(self):
        path = pathlib.Path(self.run_path) / 'config.yml'
        ConfigService.write_config_file(str(path), self.conf)

    def write_scores(self):
        results_path = pathlib.Path(self.run_path) / 'results.txt'
        if results_path.exists() and self.conf.score:
            scores_path = pathlib.Path(self.run_path) / 'scores.txt'
            qrels_config = self.conf.score.input
            scorer = Scorer(qrels_config, self.conf.score.metrics)
            scorer.score(results_path, scores_path)


class SerialJob(Job):
    """Single threaded job"""

    def _run(self):
        report = Report()
        if self.stage1:
            timer1 = Timer()
            LOGGER.info("Stage 1: Starting processing of documents")
            with timer1:
                self.stage1.run()
            report.stage1 = StageReport(self.stage1.count, self.stage1.report)
            LOGGER.info("Stage 1: Ingested %d documents", self.stage1.count)
            LOGGER.info("Stage 1 took %.1f secs", timer1.time)

        if self.stage2:
            timer2 = Timer()
            LOGGER.info("Stage 2: Starting processing of topics")
            with timer2:
                self.stage2.run()
            report.stage2 = StageReport(self.stage2.count, self.stage2.report)
            LOGGER.info("Stage 2: Processed %d topics", self.stage2.count)
            LOGGER.info("Stage 2 took %.1f secs", timer2.time)

        return report


@dataclasses.dataclass
class ParallelJobDef:
    """Describes a parallel sub-job"""
    id: int  # zero based id counter for sub-jobs
    conf: RunnerConfig


class ParallelJob(Job):
    """Parallel job that uses multiple processes.

    This uses concurrent.futures to implement map/reduce over the input iterators.
    """
    def __init__(self, conf, stage1, stage2, debug):
        super().__init__(conf, stage1, stage2)
        multiprocessing.set_start_method('spawn')  # so JVM doesn't get copied to child processes
        self.debug = debug
        self.stage1_jobs = self.stage2_jobs = None
        if stage1:
            self.stage1_jobs = self._get_stage1_jobs(conf.run.stage1.num_jobs)
        if stage2:
            self.stage2_jobs = self._get_stage2_jobs(conf.run.stage2.num_jobs)

    def _run(self):
        report1 = Report()
        report2 = Report()
        if self.stage1_jobs:
            LOGGER.info("Stage 1: Starting processing of documents")
            self.stage1.begin()
            report1 = self.map(self.stage1_jobs, self.debug)
            self.stage1.reduce()
            self.stage1.end()
            LOGGER.info("Stage 1: Ingested %d documents", report1.stage1.count)

        if self.stage2_jobs:
            LOGGER.info("Stage 2: Starting processing of queries")
            self.stage2.begin()
            report2 = self.map(self.stage2_jobs, self.debug)
            self.stage2.reduce()
            self.stage2.end()
            LOGGER.info("Stage 2: Processed %d queries", report2.stage2.count)

        return report1 + report2

    def map(self, jobs, debug):
        """
        Args:
            jobs (list of ParallelJobDef): Job definitions to be mapped over.
            debug (bool): Whether to run in debug mode.
        Returns:
            Report
        """
        func = functools.partial(self._fork, debug=debug)
        with concurrent.futures.ProcessPoolExecutor(max_workers=len(jobs)) as executor:
            # we loop in a try/except to catch errors from the jobs running in separate processes
            try:
                return sum(executor.map(func, jobs))
            except Exception as e:
                LOGGER.error(f"Parallel job failed with {e}")
                sys.exit(-1)

    @staticmethod
    def _fork(job, debug):
        # only log parallel jobs to their unique log file
        log_level = logging.DEBUG if debug else logging.INFO
        logger = logging.getLogger('patapsco')
        logger.setLevel(log_level)
        log_file = path_append(job.conf.run.path, f"patapsco.{job.id}.log")
        file = logging.FileHandler(log_file)
        file.setLevel(logger.level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file.setFormatter(formatter)
        file.addFilter(LoggingFilter())
        logger.addHandler(file)

        job = JobBuilder(job.conf).build(debug)
        return job.run(sub_job=True)

    def _get_stage1_jobs(self, num_processes):
        num_items = len(self.stage1.iterator)
        job_size = int(math.ceil(num_items / num_processes))
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

    def _get_stage2_jobs(self, num_processes):
        num_items = len(self.stage2.iterator)
        job_size = int(math.ceil(num_items / num_processes))
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
            if conf.database.output:
                conf.database.output = path_append(conf.database.output, part)
        except AttributeError:
            pass
        try:
            if conf.documents.output:
                conf.documents.output = path_append(conf.documents.output, part)
        except AttributeError:
            pass
        try:
            if conf.index.output:
                conf.index.output = path_append(conf.index.output, part)
        except AttributeError:
            pass

    @staticmethod
    def _update_stage2_output_paths(conf, part):
        # configs may not have all tasks so we ignore errors
        try:
            if conf.topics.output:
                conf.topics.output = path_append(conf.topics.output, part)
        except AttributeError:
            pass
        try:
            if conf.queries.output:
                conf.queries.output = path_append(conf.queries.output, part)
        except AttributeError:
            pass
        try:
            if conf.retrieve.output:
                conf.retrieve.output = path_append(conf.retrieve.output, part)
        except AttributeError:
            pass
        try:
            if conf.rerank.output:
                conf.rerank.output = path_append(conf.rerank.output, part)
        except AttributeError:
            pass


class QsubJob(Job):
    """Parallel job that uses qsub."""
    def __init__(self, conf, stage1, stage2, debug):
        super().__init__(conf, stage1, stage2)
        conf.run.path = str(pathlib.Path(self.run_path).absolute())
        conf.run.parallel = None
        self.base_dir = (pathlib.Path(self.run_path) / 'qsub').absolute()
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.script_path = self.base_dir / 'job.sh'
        self.config_path = self.base_dir / 'config.yml'
        self.log_path = self.base_dir / 'patapsco.log'
        ConfigService.write_config_file(self.config_path, conf)
        self._create_script(debug)

    def run(self, sub_job=False):
        LOGGER.info("Launching qsub run: %s", self.conf.run.name)
        self._move_log_file()

        args = ['qsub', '-q', 'all.q', str(self.script_path)]
        try:
            ps = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True)
            print()
            print(ps.stdout.decode())
        except subprocess.CalledProcessError as e:
            print(f"Error: {e}")

    def _create_script(self, debug):
        template_path = pathlib.Path(__file__).parent / 'resources' / 'qsub' / 'job.sh'
        template = template_path.read_text()
        debug = '-d' if debug else ''
        content = template.format(base=str(self.base_dir), config=str(self.config_path), debug=debug)
        self.script_path.write_text(content)
        self.script_path.chmod(0o755)

    def _move_log_file(self):
        logging.shutdown()
        current_log_path = pathlib.Path(self.run_path) / 'patapsco.log'
        current_log_path.rename(self.log_path)


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
        self.run_path = pathlib.Path(conf.run.path)
        self.artifact_helper = ArtifactHelper()
        self.doc_lang = None
        self.query_lang = None

    def build(self, debug):
        """Build the job(s) for this run

        Args:
            debug (bool): Debug flag.
        """
        stage1 = stage2 = None
        stage1_plan = []
        stage2_plan = []

        if is_complete(self.conf.run.path):
            raise ConfigError('Run is already complete. Delete the output directory to rerun.')

        if self.conf.run.parallel:
            LOGGER.info(f'Parallel job selected of type {self.conf.run.parallel}.')

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
            if self.conf.run.parallel.lower() == "mp":
                return ParallelJob(self.conf, stage1, stage2, debug)
            elif self.conf.run.parallel.lower() == "qsub":
                return QsubJob(self.conf, stage1, stage2, debug)
            else:
                raise ConfigError(f"Unknown parallel job type: {self.conf.run.parallel}")
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
                                         'documents.output', 'index not configured with documents')
        stage_conf = self.conf.run.stage1
        return SlicedIterator(iterator, stage_conf.start, stage_conf.stop)

    def _get_stage1_tasks(self, plan):
        # Stage 1 is generally: read docs, process them, build index.
        # For each task, we clear previous data from a failed run if it exists.
        # Then we build the tasks from the plan and configuration.
        run_path = self.conf.run.path
        tasks = []

        if Tasks.DOCUMENTS in plan:
            # doc reader -> doc processor with doc db -> optional doc writer
            self.docs_lang = LangStandardizer.standardize(self.conf.documents.input.lang)
            self.conf.documents.input.lang = self.docs_lang
            self.clear_output(self.conf.documents)
            self.clear_output(self.conf.database)
            artifact_conf = self.artifact_helper.get_config(self.conf, Tasks.DOCUMENTS)
            db = DocumentDatabaseFactory.create(run_path, self.conf.database.output, artifact_conf)
            tasks.append(DocumentProcessor(run_path, self.conf.documents, self.docs_lang, db))
            # add doc writer if user requesting that we save processed docs
            if self.conf.documents.output:
                tasks.append(DocWriter(run_path, self.conf.documents, artifact_conf))

        if Tasks.INDEX in plan:
            # indexer or processed doc reader -> indexer
            self.clear_output(self.conf.index)
            artifact_conf = self.artifact_helper.get_config(self.conf, Tasks.INDEX)
            tasks.append(IndexerFactory.create(run_path, self.conf.index, artifact_conf))

        return tasks

    def _build_stage1_pipeline(self, iterator, tasks):
        # select pipeline based on stage configuration
        stage_conf = self.conf.run.stage1
        stage_conf.progress_interval = stage_conf.progress_interval if stage_conf.progress_interval else 10000
        if self.conf.run.parallel:
            LOGGER.info(f'Stage 1 has {stage_conf.num_jobs} parallel jobs.')
        if stage_conf.mode == PipelineMode.STREAMING:
            LOGGER.info("Stage 1 is a streaming pipeline.")
            pipeline_class = StreamingPipeline
        elif stage_conf.mode == PipelineMode.BATCH:
            batch_size_char = str(stage_conf.batch_size) if stage_conf.batch_size else '∞'
            LOGGER.info("Stage 1 is a batch pipeline selected with batch size of %s.", batch_size_char)
            pipeline_class = functools.partial(BatchPipeline, n=stage_conf.batch_size)
        else:
            raise ConfigError(f"Unrecognized pipeline mode: {stage_conf.mode}")
        pipeline = pipeline_class(iterator, tasks, progress_interval=stage_conf.progress_interval)
        LOGGER.info("Stage 1 pipeline: %s", pipeline)
        return pipeline

    def _create_stage2_plan(self):
        # Analyze the config and check there are any artifacts from a previous run.
        # A plan consists of a list of Tasks to be constructed into a pipeline.
        stage2 = []
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
            if not self.is_task_complete(self.conf.rerank):
                stage2.append(Tasks.RERANK)

        # confirm that we're not missing tasks
        if Tasks.TOPICS in stage2 and Tasks.RETRIEVE in stage2 and Tasks.QUERIES not in stage2:
            raise ConfigError("Missing configuration for queries")
        if Tasks.QUERIES in stage2 and Tasks.RERANK in stage2 and Tasks.RETRIEVE not in stage2:
            raise ConfigError("Missing configuration for retrieve")

        return stage2

    def _get_stage2_iterator(self, plan):
        # Get the iterator for pipeline based on plan and configuration
        if not plan:
            return None
        if Tasks.TOPICS in plan:
            iterator = TopicReaderFactory.create(self.conf.topics.input)
        elif Tasks.QUERIES in plan:
            iterator = self._setup_input(QueryReader, 'queries.input.path', 'topics.output',
                                         'query processor not configured with input')
            query = iterator.peek()
            self.query_lang = LangStandardizer.standardize(query.lang)
        elif Tasks.RETRIEVE in plan:
            iterator = self._setup_input(QueryReader, 'retrieve.input.queries.path', 'queries.output',
                                         'retrieve not configured with queries')
            query = iterator.peek()
            self.query_lang = LangStandardizer.standardize(query.lang)
        else:
            iterator = self._setup_input(JsonResultsReader, 'rerank.input.results.path', 'retrieve.output',
                                         'rerank not configured with retrieve results')
        stage_conf = self.conf.run.stage2
        return SlicedIterator(iterator, stage_conf.start, stage_conf.stop)

    def _get_stage2_tasks(self, plan):
        # Stage 2 is generally: read topics, extract query, process them, retrieve results, rerank them.
        # For each task, we clear previous data from a failed run if it exists.
        # Then we build the tasks from the plan and configuration.
        run_path = self.conf.run.path
        tasks = []

        if Tasks.TOPICS in plan:
            # topic reader -> topic processor -> optional query writer
            self.query_lang = LangStandardizer.standardize(self.conf.topics.input.lang)
            self.conf.topics.input.lang = self.query_lang
            self.clear_output(self.conf.topics)
            artifact_conf = self.artifact_helper.get_config(self.conf, Tasks.TOPICS)
            tasks.append(TopicProcessor(run_path, self.conf.topics))
            if self.conf.topics.output:
                tasks.append(QueryWriter(run_path, self.conf.topics, artifact_conf))

        if Tasks.QUERIES in plan:
            # optional query reader -> query processor -> optional query writer
            self.clear_output(self.conf.queries)
            tasks.append(QueryProcessor(run_path, self.conf.queries, self.query_lang))
            artifact_conf = self.artifact_helper.get_config(self.conf, Tasks.QUERIES)
            if self.conf.queries.output:
                tasks.append(QueryWriter(run_path, self.conf.queries, artifact_conf))

        if Tasks.RETRIEVE in plan:
            self.clear_output(self.conf.retrieve)
            if not self.conf.index:
                # copy in the configuration that created the index (this path is always set in the ConfigPreprocessor)
                self.artifact_helper.combine(self.conf, self.conf.retrieve.input.index.path)
            artifact_conf = self.artifact_helper.get_config(self.conf, Tasks.RETRIEVE)
            tasks.append(RetrieverFactory.create(run_path, self.conf.retrieve))
            if self.conf.retrieve.output:
                tasks.append(JsonResultsWriter(run_path, self.conf.retrieve, artifact_conf))

        if Tasks.RERANK in plan:
            self.clear_output(self.conf.rerank)
            artifact_conf = self.artifact_helper.get_config(self.conf, Tasks.RERANK)
            db = DocumentDatabaseFactory.create(run_path, self.conf.rerank.input.db.path, readonly=True)
            tasks.append(RerankFactory.create(run_path, self.conf.rerank, db))
            if self.conf.rerank.output:
                tasks.append(JsonResultsWriter(run_path, self.conf.rerank, artifact_conf))

        if Tasks.RETRIEVE in plan or Tasks.RERANK in plan:
            tasks.append(TrecResultsWriter(self.conf))

        return tasks

    def _build_stage2_pipeline(self, iterator, tasks):
        # select pipeline based on stage configuration
        stage_conf = self.conf.run.stage2
        stage_conf.progress_interval = stage_conf.progress_interval if stage_conf.progress_interval else 10
        if self.conf.run.parallel:
            LOGGER.info(f'Stage 2 has {stage_conf.num_jobs} parallel jobs.')

        if stage_conf.mode == PipelineMode.STREAMING:
            LOGGER.info("Stage 2 is a streaming pipeline.")
            pipeline_class = StreamingPipeline
        elif stage_conf.mode == PipelineMode.BATCH:
            batch_size_char = str(stage_conf.batch_size) if stage_conf.batch_size else '∞'
            LOGGER.info("Stage 2 is a batch pipeline selected with batch size of %s.", batch_size_char)
            pipeline_class = functools.partial(BatchPipeline, n=stage_conf.batch_size)
        else:
            raise ConfigError(f"Unrecognized pipeline mode: {stage_conf.mode}")
        pipeline = pipeline_class(iterator, tasks, progress_interval=stage_conf.progress_interval)
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
            path = pathlib.Path(self.run_path / obj)
            self.artifact_helper.combine(self.conf, path)
            return cls(path)
        except AttributeError:
            obj = self.conf
            fields = output_path.split('.')
            try:
                while fields:
                    field = fields.pop(0)
                    obj = getattr(obj, field)
                path = pathlib.Path(self.run_path / obj)
                self.artifact_helper.combine(self.conf, path)
                return cls(path)
            except AttributeError:
                raise ConfigError(error_msg)

    def is_task_complete(self, task_conf):
        """Checks whether the task is already complete"""
        if task_conf is None or not task_conf.output:
            return False
        path = self.run_path / task_conf.output
        return is_complete(path)

    def clear_output(self, task_conf):
        """Delete the output directory if previous run did not complete

        Args:
            task_conf (BaseConfig): Configuration for a task.
        """
        if task_conf.output:
            path = self.run_path / task_conf.output
            if path.exists():
                delete_dir(path)

    def check_sources_of_documents(self):
        """The docs in the index and database must come from the same source"""
        config_path = pathlib.Path(self.conf.run.path) / self.conf.rerank.input.db.path / 'config.yml'
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
        """The docs and queries must have the same text processing"""
        doc = self.conf.documents.process
        query = self.conf.queries.process
        try:
            assert doc.normalize.lowercase == query.normalize.lowercase
            assert doc.tokenize == query.tokenize
            assert doc.stopwords == query.stopwords
            assert doc.stem == query.stem
        except AssertionError:
            raise ConfigError("Text processing for documents and queries does not match")
