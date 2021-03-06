import copy
import logging
import logging.handlers
import pathlib

from .__version__ import __version__
from .helpers import ConfigHelper
from .job import JobBuilder, JobType
from .util import LoggingFilter

LOGGER = logging.getLogger(__name__)


class Runner:
    def __init__(self, config, debug=False, overrides=None, job_type=JobType.NORMAL, **kwargs):
        """
        Args:
            config (str|dict): path to configuration file or a configuration dictionary
            debug (bool): whether to run in debug mode
            overrides (list): Optional list of key value pair overrides of config parameters
            job_type (JobType): internal parameter to select between jobs
            **kwargs (dict): keyword args
        """
        self.job_type = job_type
        self.setup_logging(debug, job_type)
        LOGGER.info(f"Patapsco version {__version__}")
        if isinstance(config, str):
            LOGGER.info(f"Configuration: {pathlib.Path(config).absolute()}")
            conf = ConfigHelper.load(config, overrides)
        else:
            # using a config dictionary
            conf = ConfigHelper.prepare(copy.deepcopy(config), overrides)
        LOGGER.info(f"Writing output to: {pathlib.Path(conf.run.path).absolute()}")
        if job_type == JobType.NORMAL:
            # no need to log to file with grid jobs
            self.add_file_logging(conf.run.path)
        self.job = JobBuilder(conf, job_type, **kwargs).build(debug)

    def run(self):
        sub_job_flag = self.job_type == JobType.MAP
        self.job.run(sub_job=sub_job_flag)

    @staticmethod
    def setup_logging(debug, job_type):
        log_level = logging.DEBUG if debug else logging.INFO
        logger = logging.getLogger('patapsco')
        initialized = bool(logger.handlers)
        logger.setLevel(log_level)
        console = logging.StreamHandler()
        console.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console.setFormatter(formatter)
        console.addFilter(LoggingFilter())
        if not initialized:
            # if using this as a library, we only want to add console logging once
            logger.addHandler(console)
        if job_type == JobType.NORMAL:
            buffer = logging.handlers.MemoryHandler(1024)
            buffer.setLevel(log_level)
            buffer.setFormatter(formatter)
            buffer.addFilter(LoggingFilter())
            if initialized:
                logger.handlers[1] = buffer
            else:
                logger.addHandler(buffer)

    @staticmethod
    def add_file_logging(path):
        logger = logging.getLogger('patapsco')
        path = pathlib.Path(path)
        path.mkdir(parents=True, exist_ok=True)
        file = logging.FileHandler(path / 'patapsco.log')
        file.setLevel(logger.level)
        file.setFormatter(logger.handlers[0].formatter)
        logger.handlers[1].setTarget(file)
        logger.handlers[1].flush()
        logger.handlers[1] = file
