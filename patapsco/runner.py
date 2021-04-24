import json
import logging
import logging.handlers
import pathlib

from .__version__ import __version__
from .builder import ConfigPreprocessor, PipelineBuilder
from .config import ConfigService
from .util import Timer

LOGGER = logging.getLogger(__name__)


class Runner:
    def __init__(self, config_filename, verbose=False, overrides=None):
        self.setup_logging(verbose)
        LOGGER.info(f"Patapsco version {__version__}")
        self.conf = ConfigPreprocessor.process(config_filename, overrides)
        builder = PipelineBuilder(self.conf)
        self.stage1, self.stage2 = builder.build()
        self.add_file_logging(self.conf.run.path)

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
        buffer = logging.handlers.MemoryHandler(1024)
        buffer.setLevel(log_level)
        buffer.setFormatter(formatter)
        logger.addHandler(buffer)

    @staticmethod
    def add_file_logging(path):
        logger = logging.getLogger('patapsco')
        file = logging.FileHandler(pathlib.Path(path) / 'patapsco.log')
        file.setLevel(logger.level)
        file.setFormatter(logger.handlers[0].formatter)
        logger.handlers[1].setTarget(file)
        logger.handlers[1].flush()
        logger.handlers[1] = file

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
