import concurrent.futures
import dataclasses
import enum
import functools
import json
import logging
import math
import multiprocessing
import pathlib
import sys
import subprocess

import psutil

from .config import ConfigService
from .database import DatabaseWriter, DocumentDatabaseFactory
from .docs import DocumentProcessor, DocumentReaderFactory, DocReader, DocWriter
from .error import ConfigError, PatapscoError
from .helpers import ArtifactHelper
from .index import IndexerFactory
from .pipeline import BatchPipeline, StreamingPipeline
from .rerank import RerankFactory
from .results import JsonResultsWriter, JsonResultsReader, TrecResultsWriter
from .retrieve import RetrieverFactory
from .schema import RunnerConfig, PipelineMode, Tasks
from .score import Scorer
from .topics import TopicProcessor, TopicReaderFactory, QueryProcessor, QueryReader, QueryWriter
from .util import DataclassJSONEncoder, get_human_readable_size, ignore_exception, LangStandardizer, LoggingFilter,\
    SlicedIterator, Timer
from .util.file import delete_dir, is_complete, is_dir_empty, path_append, touch_complete

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

    def __init__(self, conf, record_conf, stage1, stage2):
        """
        Args:
            conf (RunConfig): Config for this job
            record_conf (RunConfig): Config for the output (includes previous partial runs like a prebuilt index)
            stage1 (Pipeline): Stage 1 pipeline or false
            stage2 (Pipeline): Stage 2 pipeline or false
        """
        self.conf = conf
        self.record_conf = record_conf
        self.run_path = conf.run.path
        self.stage1 = stage1
        self.stage2 = stage2

    def run(self, sub_job=False):
        LOGGER.info("Starting run: %s", self.conf.run.name)

        report = self._run()

        if not sub_job:
            self.write_complete()
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

    def write_complete(self):
        # run is only complete if we have results
        results_path = pathlib.Path(self.run_path) / self.conf.run.results
        if results_path.exists():
            touch_complete(self.run_path)

    def write_config(self):
        # if this run starts with artifacts, also write out their config in full config file
        path = pathlib.Path(self.run_path) / 'config.yml'
        ConfigService.write_config_file(str(path), self.conf)
        if self.conf != self.record_conf:
            path = pathlib.Path(self.run_path) / 'config_full.yml'
            ConfigService.write_config_file(str(path), self.record_conf)

    def write_scores(self):
        results_path = pathlib.Path(self.run_path) / self.conf.run.results
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
class MultiprocessingJobDef:
    """Describes a multiprocessing parallel sub-job"""
    id: int  # zero based id counter for sub-jobs
    conf: RunnerConfig


class MultiprocessingJob(Job):
    """Multiprocessing parallel job.

    This uses concurrent.futures to implement map/reduce over the input iterators.
    """
    def __init__(self, conf, record_conf, stage1, stage2, debug):
        super().__init__(conf, record_conf, stage1, stage2)
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
            timer1 = Timer()
            with timer1:
                self.stage1.begin()
                report1 = self.map(self.stage1_jobs, self.debug)
                self.stage1.reduce()
                self.stage1.end()
                self._del_reduce_directories()
            LOGGER.info("Stage 1: Ingested %d documents", report1.stage1.count)
            LOGGER.info("Stage 1 took %.1f secs", timer1.time)

        if self.stage2_jobs:
            LOGGER.info("Stage 2: Starting processing of queries")
            timer2 = Timer()
            with timer2:
                self.stage2.begin()
                report2 = self.map(self.stage2_jobs, self.debug)
                self.stage2.reduce()
                self.stage2.end()
                self._del_reduce_directories()
            LOGGER.info("Stage 2: Processed %d queries", report2.stage2.count)
            LOGGER.info("Stage 2 took %.1f secs", timer2.time)

        return report1 + report2

    def map(self, jobs, debug):
        """
        Args:
            jobs (list of MultiprocessingJobDef): Job definitions to be mapped over.
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
                raise PatapscoError(f"multiprocessing map failed from {type(e).__name__} {e}") from e

    @staticmethod
    def _fork(job, debug):
        # only log parallel jobs to their unique log file
        log_level = logging.DEBUG if debug else logging.INFO
        logger = logging.getLogger('patapsco')
        logger.setLevel(log_level)
        log_dir = pathlib.Path(job.conf.run.path) / 'logs'
        log_dir.mkdir(exist_ok=True)
        stage = 'stage1' if job.conf.run.stage1 else 'stage2'
        log_file = path_append(log_dir, f"patapsco.{stage}.{job.id}.log")
        file = logging.FileHandler(log_file)
        file.setLevel(logger.level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file.setFormatter(formatter)
        file.addFilter(LoggingFilter())
        logger.addHandler(file)

        job = JobBuilder(job.conf, JobType.NORMAL).build(debug)
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
            stage1_jobs.append(MultiprocessingJobDef(part, conf))
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
            stage2_jobs.append(MultiprocessingJobDef(part, conf))
        return stage2_jobs

    @staticmethod
    def _update_stage1_output_paths(conf, part):
        # configs may not have all tasks so we ignore errors
        with ignore_exception(AttributeError):
            if conf.database.output:
                conf.database.output = path_append(part, conf.database.output)
        with ignore_exception(AttributeError):
            if conf.documents.output:
                conf.documents.output = path_append(part, conf.documents.output)
        with ignore_exception(AttributeError):
            if conf.index.output:
                conf.index.output = path_append(part, conf.index.output)

    @staticmethod
    def _update_stage2_output_paths(conf, part):
        conf.run.results = path_append(part, conf.run.results)
        # configs may not have all tasks so we ignore errors
        with ignore_exception(AttributeError):
            if conf.topics.output:
                conf.topics.output = path_append(part, conf.topics.output)
        with ignore_exception(AttributeError):
            if conf.queries.output:
                conf.queries.output = path_append(part, conf.queries.output)
        with ignore_exception(AttributeError):
            if conf.retrieve.output:
                conf.retrieve.output = path_append(part, conf.retrieve.output)
        with ignore_exception(AttributeError):
            if conf.rerank.output:
                conf.rerank.output = path_append(part, conf.rerank.output)

    def _del_reduce_directories(self):
        base_dir = pathlib.Path(self.run_path)
        [delete_dir(item) for item in base_dir.glob('part*')]


class ClusterJob(Job):
    """Parallel job that uses qsub or slurm."""
    def __init__(self, conf, record_conf, stage1, stage2, debug):
        super().__init__(conf, record_conf, stage1, stage2)
        self.debug = debug
        conf.run.path = str(pathlib.Path(self.run_path).absolute())
        self.cluster_config = conf.run.parallel.copy()
        self.scheduler = 'qsub' if self.cluster_config.name == 'qsub' else 'sbatch'
        self.email = self._prepare_email()
        self.base_dir = (pathlib.Path(self.run_path) / self.scheduler).absolute()
        try:
            self.base_dir.mkdir(parents=True)
        except FileExistsError:
            raise ConfigError(f"A {self.scheduler} directory already exists at {self.base_dir}")
        self.stage1_map_path = self.base_dir / 'stage1_map.sh'
        self.stage2_map_path = self.base_dir / 'stage2_map.sh'
        self.stage1_reduce_path = self.base_dir / 'stage1_reduce.sh'
        self.stage2_reduce_path = self.base_dir / 'stage2_reduce.sh'
        self.config_path = self.base_dir / 'config.yml'
        self.log_path = self.base_dir / 'patapsco.log'
        ConfigService.write_config_file(self.config_path, conf)
        self._create_map_scripts(debug)
        self._create_reduce_scripts(debug)

    def run(self, sub_job=False):
        LOGGER.info(f"Launching {self.scheduler} run: {self.conf.run.name}")

        job_id = None
        if self.stage1:
            job_id = self._launch_job(self.stage1_map_path)
            LOGGER.info(f"Job {job_id} submitted - stage 1 mapper")
            job_id = self._launch_job(self.stage1_reduce_path, job_id)
            LOGGER.info(f"Job {job_id} submitted - stage 1 reducer")
        if self.stage2:
            job_id = self._launch_job(self.stage2_map_path, job_id)
            LOGGER.info(f"Job {job_id} submitted - stage 2 mapper")
            job_id = self._launch_job(self.stage2_reduce_path, job_id)
            LOGGER.info(f"Job {job_id} submitted - stage 2 reducer")
        LOGGER.info("All jobs submitted")

    def _launch_job(self, script_path, hold=None):
        """Launch a cluster job and return the job id"""
        args = self._create_arguments(hold)
        args.append(str(script_path))
        if self.debug:
            LOGGER.debug(' '.join([str(arg) for arg in args]))
        try:
            ps = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True)
            return self._extract_job_id(ps.stdout.decode())
        except subprocess.CalledProcessError as e:
            print(f"Error: {e}")
            sys.exit(-1)

    def _create_arguments(self, hold_id=None):
        if self.scheduler == 'qsub':
            args = ['qsub', '-terse', '-q', self.cluster_config.queue]
            if hold_id:
                args.extend(['-hold_jid', hold_id])
        else:
            args = ['sbatch', '-p', self.cluster_config.queue]
            if hold_id:
                args.append(f"--depend=afterok:{hold_id}")
        return args

    def _extract_job_id(self, stdout):
        if self.scheduler == 'qsub':
            # array jobs have ids of xxxxxxx.1-num_jobs
            job_id = stdout.strip().split('.')[0]
        else:
            job_id = stdout.strip().split()[-1].split('.')[0]
        return job_id

    def _prepare_resources(self):
        if self.scheduler == 'qsub':
            # qsub accepts a command separate list of resources that include cpus, gpus, memory, and time
            resources = f"#$ -l {self.cluster_config.resources}"
        else:
            # sbatch has different lines for different types of resources (time vs gpu)
            resources = [f"#SBATCH {resource.strip()}" for resource in self.cluster_config.resources.split(',')]
            resources = "\n".join(resources)
        return resources

    def _prepare_email(self):
        if self.scheduler == 'qsub':
            email = f"#$ -m ea -M {self.cluster_config.email}" if self.cluster_config.email else ''
        else:
            email = ''
            if self.cluster_config.email:
                email = f"#SBATCH --mail-user={self.cluster_config.email}\n#SBATCH --mail-type=END,FAIL"
        return email

    def _create_map_scripts(self, debug):
        template_path = pathlib.Path(__file__).parent / 'resources' / self.scheduler / 'map.sh'
        template = template_path.read_text()
        debug = '-d' if debug else ''
        code = self.cluster_config.code if self.cluster_config.code else ''
        if self.stage1:
            num_jobs = self.conf.run.stage1.num_jobs
            LOGGER.debug(f"Stage 1 is using {num_jobs} jobs")
            increment = self._get_stage1_increment(num_jobs)
            content = template.format(
                base=str(self.base_dir),
                code=code,
                config=str(self.config_path),
                debug=debug,
                increment=increment,
                num_jobs=num_jobs,
                resources=self._prepare_resources(),
                stage=1
            )
            self.stage1_map_path.write_text(content)
            self.stage1_map_path.chmod(0o755)
        if self.stage2:
            num_jobs = self.conf.run.stage2.num_jobs
            LOGGER.debug(f"Stage 2 is using {num_jobs} jobs")
            increment = self._get_stage2_increment(num_jobs)
            content = template.format(
                base=str(self.base_dir),
                code=code,
                config=str(self.config_path),
                debug=debug,
                increment=increment,
                num_jobs=num_jobs,
                resources=self._prepare_resources(),
                stage=2
            )
            self.stage2_map_path.write_text(content)
            self.stage2_map_path.chmod(0o755)

    def _create_reduce_scripts(self, debug):
        template_path = pathlib.Path(__file__).parent / 'resources' / self.scheduler / 'reduce.sh'
        template = template_path.read_text()
        debug = '-d' if debug else ''
        code = self.cluster_config.code if self.cluster_config.code else ''
        if self.stage1:
            content = template.format(
                base=str(self.base_dir),
                code=code,
                config=str(self.config_path),
                debug=debug,
                email=self.email,
                resources=self._prepare_resources(),
                stage=1
            )
            self.stage1_reduce_path.write_text(content)
            self.stage1_reduce_path.chmod(0o755)
        if self.stage2:
            content = template.format(
                base=str(self.base_dir),
                code=code,
                config=str(self.config_path),
                debug=debug,
                email=self.email,
                resources=self._prepare_resources(),
                stage=2
            )
            self.stage2_reduce_path.write_text(content)
            self.stage2_reduce_path.chmod(0o755)

    def _get_stage1_increment(self, num_jobs):
        LOGGER.info("Calculating job size...")
        num_items = len(self.stage1.iterator)
        return int(math.ceil(num_items / num_jobs))

    def _get_stage2_increment(self, num_jobs):
        num_items = len(self.stage2.iterator)
        return int(math.ceil(num_items / num_jobs))


class ReduceJob(Job):
    """Reduce job run on cluster"""

    def __init__(self, conf, record_conf, stage1, stage2, debug):
        super().__init__(conf, record_conf, stage1, stage2)
        self.debug = debug
        self.scheduler = 'qsub' if conf.run.parallel.name == 'qsub' else 'sbatch'
        self.job_dir = (pathlib.Path(self.run_path) / self.scheduler).absolute()

    def _run(self):
        if self.stage1:
            LOGGER.info("Stage 1: Running reduce")
            timer1 = Timer()
            with timer1:
                self.stage1.begin()
                self.stage1.reduce()
                self.stage1.end()
                self._del_reduce_directories()
            LOGGER.info("Stage 1 reduce took %.1f secs", timer1.time)
            self._collect_warnings()
            self._collect_memory_and_time()

        if self.stage2:
            LOGGER.info("Stage 2: Running reduce")
            timer2 = Timer()
            with timer2:
                self.stage2.begin()
                self.stage2.reduce()
                self.stage2.end()
                self._del_reduce_directories()
            LOGGER.info("Stage 2 reduce took %.1f secs", timer2.time)
            self._collect_warnings()
            self._collect_memory_and_time()

        return Report()

    def _del_reduce_directories(self):
        base_dir = pathlib.Path(self.run_path)
        [delete_dir(item) for item in base_dir.glob('part*')]

    def _collect_warnings(self):
        # only create the file if there are warnings or errors
        logs = self.job_dir / "*"
        output = pathlib.Path(self.run_path) / "warnings.txt"
        try:
            subprocess.run(f"grep -q -e WARNING -e ERROR {logs}", shell=True, check=True)
            subprocess.run(f"grep -h -e WARNING -e ERROR {logs} > {output}", shell=True)
        except subprocess.CalledProcessError:
            pass

    def _collect_memory_and_time(self):
        logs = self.job_dir / "*"
        output = pathlib.Path(self.run_path) / "memory_and_time.log"
        try:
            subprocess.run(f"grep -h secs {logs} > {output}", shell=True)
            subprocess.run(f"grep -h Memory {logs} >> {output}", shell=True)
        except subprocess.CalledProcessError:
            pass


class JobType(enum.Enum):
    """Patapsco supports map reduce for parallel cluster jobs or normal local runs"""
    NORMAL = enum.auto()
    MAP = enum.auto()
    REDUCE = enum.auto()


class JobBuilder:
    """Builds a Job based on stage 1 and stage 2 pipelines

    Analyzes the configuration to create a plan of which tasks to include.
    Then builds the pipelines based on the plan and configuration.
    Handles restarting a run where it left off.
    Will create pipelines for partial runs (that end early or start from artifacts).
    """
    def __init__(self, conf, job_type=JobType.NORMAL, **kwargs):
        """
        Args:
            conf (RunnerConfig): Configuration for the runner.
            job_type (JobType): Normal, map, reduce
        """
        self.conf = conf
        self.record_conf = conf.copy(deep=True)
        self.parallel_args = kwargs
        self.run_path = pathlib.Path(conf.run.path)
        self.artifact_helper = ArtifactHelper()
        self.doc_lang = None
        self.query_lang = None
        self.job_type = job_type
        if job_type == JobType.MAP:
            self._update_config_for_grid_jobs()

    def _update_config_for_grid_jobs(self):
        """Update config based on parallel args"""
        if self.parallel_args['stage'] == 1:
            self.conf.run.stage2 = False
            self.conf.run.stage1.start = self.parallel_args['increment'] * self.parallel_args['job']
            self.conf.run.stage1.stop = self.parallel_args['increment'] * (self.parallel_args['job'] + 1)
            part = f"part_{self.parallel_args['job']}"
            with ignore_exception(AttributeError):
                if self.conf.database.output:
                    self.conf.database.output = path_append(part, self.conf.database.output)
            with ignore_exception(AttributeError):
                if self.conf.documents.output:
                    self.conf.documents.output = path_append(part, self.conf.documents.output)
            with ignore_exception(AttributeError):
                if self.conf.index.output:
                    self.conf.index.output = path_append(part, self.conf.index.output)
        else:
            self.conf.run.stage1 = False
            self.conf.run.stage2.start = self.parallel_args['increment'] * self.parallel_args['job']
            self.conf.run.stage2.stop = self.parallel_args['increment'] * (self.parallel_args['job'] + 1)
            part = f"part_{self.parallel_args['job']}"
            self.conf.run.results = path_append(part, self.conf.run.results)
            # configs may not have all tasks so we ignore errors
            with ignore_exception(AttributeError):
                if self.conf.topics.output:
                    self.conf.topics.output = path_append(part, self.conf.topics.output)
            with ignore_exception(AttributeError):
                if self.conf.queries.output:
                    self.conf.queries.output = path_append(part, self.conf.queries.output)
            with ignore_exception(AttributeError):
                if self.conf.retrieve.output:
                    self.conf.retrieve.output = path_append(part, self.conf.retrieve.output)
            with ignore_exception(AttributeError):
                if self.conf.rerank.output:
                    self.conf.rerank.output = path_append(part, self.conf.rerank.output)

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
            LOGGER.info(f'Parallel job selected of type {self.conf.run.parallel.name}.')

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
        if stage2 and Tasks.RETRIEVE in stage2_plan and self.record_conf.queries.process.strict_check:
            self.check_text_processing()

        if self.job_type == JobType.MAP:
            # Map jobs are always plain serial jobs
            return SerialJob(self.conf, self.record_conf, stage1, stage2)
        elif self.job_type == JobType.REDUCE:
            # Reduce jobs have their own type
            if self.parallel_args['stage'] == 1:
                return ReduceJob(self.conf, self.record_conf, stage1, None, debug)
            else:
                return ReduceJob(self.conf, self.record_conf, None, stage2, debug)
        elif self.conf.run.parallel:
            # this is the parent job for multiprocessing, qsub, etc.
            parallel_type = self.conf.run.parallel.name.lower()
            if parallel_type == "mp":
                return MultiprocessingJob(self.conf, self.record_conf, stage1, stage2, debug)
            elif parallel_type == "qsub" or parallel_type == "sbatch":
                return ClusterJob(self.conf, self.record_conf, stage1, stage2, debug)
            else:
                raise ConfigError(f"Unknown parallel job type: {self.conf.run.parallel.name}")
        else:
            # plain old single threaded job
            return SerialJob(self.conf, self.record_conf, stage1, stage2)

    def _create_stage1_plan(self):
        # Analyze the config and check there are any artifacts from a previous run.
        # A plan consists of a list of Tasks to be constructed into a pipeline.
        stage1 = []
        index_complete = self.conf.index and self.is_task_complete(self.conf.index)
        if self.conf.documents:
            if not self.is_task_complete(self.conf.documents) and not index_complete:
                stage1.append(Tasks.DOCUMENTS)
        if self.conf.database and self.conf.database.output and not self.is_task_complete(self.conf.database):
            stage1.append(Tasks.DATABASE)
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
            # doc reader -> doc processor
            self.docs_lang = LangStandardizer.standardize(self.conf.documents.input.lang)
            self.conf.documents.input.lang = self.docs_lang
            self.clear_output(self.conf.documents)
            doc_artifact_conf = self.artifact_helper.get_config(self.conf, Tasks.DOCUMENTS)
            tasks.append(DocumentProcessor(run_path, self.conf.documents, self.docs_lang))

        if Tasks.DATABASE in plan:
            self.clear_output(self.conf.database)
            artifact_conf = self.artifact_helper.get_config(self.conf, Tasks.DATABASE)
            tasks.append(DatabaseWriter(run_path, self.conf.database, artifact_conf))

        if Tasks.DOCUMENTS in plan and self.conf.documents.output:
            # add doc writer if user requesting that we save processed docs
            tasks.append(DocWriter(run_path, self.conf.documents, doc_artifact_conf))

        if Tasks.INDEX in plan:
            # indexer or processed doc reader -> indexer
            self.clear_output(self.conf.index)
            artifact_conf = self.artifact_helper.get_config(self.conf, Tasks.INDEX)
            tasks.append(IndexerFactory.create(run_path, self.conf.index, artifact_conf))

        return tasks

    def _build_stage1_pipeline(self, iterator, tasks):
        # select pipeline based on stage configuration
        stage_conf = self.conf.run.stage1
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
                                         'query processor not configured with input',
                                         False)
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
                self.artifact_helper.combine(self.record_conf, self.conf.retrieve.input.index.path)
            artifact_conf = self.artifact_helper.get_config(self.conf, Tasks.RETRIEVE)
            tasks.append(RetrieverFactory.create(run_path, self.conf.retrieve))
            if self.conf.retrieve.output:
                tasks.append(JsonResultsWriter(run_path, self.conf.retrieve, artifact_conf))

        if Tasks.RERANK in plan:
            self.clear_output(self.conf.rerank)
            if not self.conf.database:
                # copy in the configuration that created the db
                self.artifact_helper.combine(self.record_conf, self.conf.rerank.input.database.path)
            artifact_conf = self.artifact_helper.get_config(self.conf, Tasks.RERANK)
            db = DocumentDatabaseFactory.create(run_path, self.conf.rerank.input.database.path, readonly=True)
            tasks.append(RerankFactory.create(run_path, self.conf.rerank, db))
            if self.conf.rerank.output:
                tasks.append(JsonResultsWriter(run_path, self.conf.rerank, artifact_conf))

        if Tasks.RETRIEVE in plan or Tasks.RERANK in plan:
            tasks.append(TrecResultsWriter(self.conf))

        return tasks

    def _build_stage2_pipeline(self, iterator, tasks):
        # select pipeline based on stage configuration
        stage_conf = self.conf.run.stage2
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

    def _setup_input(self, cls, input_path, output_path, error_msg, required=True):
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
            self.artifact_helper.combine(self.record_conf, path, required)
            return cls(path)
        except AttributeError:
            obj = self.conf
            fields = output_path.split('.')
            try:
                while fields:
                    field = fields.pop(0)
                    obj = getattr(obj, field)
                path = pathlib.Path(self.run_path / obj)
                self.artifact_helper.combine(self.record_conf, path)
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
            if path.exists() and not is_dir_empty(path):
                if path.is_symlink():
                    # we don't want to unlink symbolic link directory so delete contents
                    for f in path.glob("*"):
                        try:
                            delete_dir(f)
                        except OSError:
                            f.unlink()
                else:
                    delete_dir(path)

    def check_sources_of_documents(self):
        """The docs in the index and database must come from the same source"""
        config_path = pathlib.Path(self.conf.run.path) / self.conf.rerank.input.database.path / 'config.yml'
        try:
            artifact_config_dict = ConfigService().read_config_file(config_path)
        except FileNotFoundError:
            LOGGER.warning("Unable to load config for the document database")
            return
        artifact_config = RunnerConfig(**artifact_config_dict)
        if not isinstance(self.record_conf.documents.input.path, type(artifact_config.documents.input.path)):
            raise ConfigError("documents in index do not match documents in database")
        if isinstance(self.record_conf.documents.input.path, str):
            name1 = pathlib.Path(self.record_conf.documents.input.path).name
            name2 = pathlib.Path(artifact_config.documents.input.path).name
            if name1 != name2:
                raise ConfigError("documents in index do not match documents in database")
        elif isinstance(self.record_conf.documents.input.path, list):
            for p1, p2 in zip(self.record_conf.documents.input.path, artifact_config.documents.input.path):
                name1 = pathlib.Path(p1).name
                name2 = pathlib.Path(p2).name
                if name1 != name2:
                    raise ConfigError("documents in index do not match documents in database")

    def check_text_processing(self):
        """The docs and queries must have the same text processing"""
        doc = self.record_conf.documents.process
        query = self.record_conf.queries.process
        try:
            assert doc.normalize.lowercase == query.normalize.lowercase
            assert doc.tokenize == query.tokenize
            assert doc.stopwords == query.stopwords
            assert doc.stem == query.stem
        except AssertionError:
            raise ConfigError("Text processing for documents and queries does not match")
