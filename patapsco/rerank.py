import copy
import itertools
import json
import pathlib
import logging
import subprocess
from tempfile import TemporaryDirectory

from .error import BadDataError, ConfigError, PatapscoError
from .pipeline import Task
from .results import Results, TrecResultsReader
from .schema import RerankConfig
from .util import TaskFactory, DataclassJSONEncoder

LOGGER = logging.getLogger(__name__)


class RerankFactory(TaskFactory):
    classes = {
        'mock': 'MockReranker',
        'shell': 'ShellReranker'
    }
    config_class = RerankConfig


class Reranker(Task):
    """Rerank interface"""

    def __init__(self, run_path, config, db):
        """
        Args:
            run_path (str): Root directory of the run.
            config (RerankConfig): Configuration parameters
            db (DocumentDatabase): Document database
        """
        super().__init__(run_path)
        self.config = config
        self.db = db

    def process(self, results):
        """Rerank query results

        Args:
            results (Results)

        Returns:
            Results
        """
        pass


class MockReranker(Reranker):
    """Mock reranker for testing

    Config requirements: none
    """

    def process(self, results):
        new_results = copy.deepcopy(results.results)
        # retrieve documents and pop one to exercise db
        try:
            docs = [self.db[result.doc_id] for result in new_results]
            if docs:
                docs.pop()
        except BadDataError as e:
            LOGGER.error(str(e))
        return Results(results.query, results.doc_lang, 'MockReranker', new_results)


class ShellReranker(Reranker):
    """Calls a shell script for reranking

    Writes the current results to a json file for shell script to read.
    Provides the output path to script and reads the output when it is done.

    Config requirements:
     - script: preferably the full path to the script

    The script is called like so:
      /path/to/script doc_lang query_lang doc_db input_file output_file

    Arbitrary options can be added to the rerank config and will be passed as --key value.
    """

    def __init__(self, run_path, config, db):
        if not pathlib.Path(config.script).exists():
            raise ConfigError(f"Reranker shell script does not exist: {config.script}")
        super().__init__(run_path, config, db)
        if config.output:
            self.dir = pathlib.Path(run_path) / config.output / 'shell'
        else:
            self.temp_dir = TemporaryDirectory()
            self.dir = pathlib.Path(self.temp_dir.name)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.batch = 0

    def __del__(self):
        if hasattr(self, 'temp_dir'):
            self.temp_dir.cleanup()

    def process(self, results):
        raise ConfigError("Shell reranker only runs with a batch pipeline")

    def batch_process(self, items):
        """ Call shell script to rerank the results from the retriever.

        Args:
            items (list of Results)
        """
        self.batch += 1
        query_lang = self._get_query_lang(items)
        doc_lang = self._get_doc_lang(items)
        input_path = str(self.dir / f"input_{self.batch}.jsonl")
        output_path = str(self.dir / f"output_{self.batch}.txt")
        log_path = str(self.dir / f"log_{self.batch}.log")
        self._write_input(items, input_path)
        args = self._create_args(doc_lang, query_lang, input_path, output_path)
        try:
            record = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True)
            self._write_log(log_path, record.args, record.stdout)
        except subprocess.CalledProcessError as e:
            self._write_log(log_path, e.cmd, e.output)
            raise PatapscoError(e)

        new_items = self._read_output(output_path, query_lang)
        if len(items) != len(new_items):
            raise PatapscoError(f"Mismatch between queries in input and output for {self.config.script}")
        return new_items

    def _create_args(self, doc_lang, query_lang, input_path, output_path):
        # get fields not included in the config definition and add them after the script path
        fields = set(RerankConfig.__fields__.keys())
        attributes = [attribute for attribute in self.config.__fields_set__ if attribute not in fields]
        pairs = [['--' + attribute, str(getattr(self.config, attribute))] for attribute in attributes]
        pairs = itertools.chain(*pairs)
        # make sure all paths are absolute
        db_path = str(pathlib.Path(self.db.path).absolute())
        input_path = str(pathlib.Path(input_path).absolute())
        output_path = str(pathlib.Path(output_path).absolute())
        # form command line args
        args = [self.config.script]
        args.extend(pairs)
        args.extend([doc_lang, query_lang, db_path, input_path, output_path])
        return args

    @staticmethod
    def _write_input(items, input_path):
        with open(input_path, 'w') as fp:
            for item in items:
                fp.write(json.dumps(item, cls=DataclassJSONEncoder))
                fp.write("\n")

    @staticmethod
    def _read_output(output_path, lang):
        reader = TrecResultsReader(output_path, lang=lang)
        return [item for item in reader]

    @staticmethod
    def _write_log(log_path, args, output):
        with open(log_path, 'wb') as fp:
            fp.write(f"command: {' '.join(args)}\n\n".encode())
            fp.write(output)

    @staticmethod
    def _get_query_lang(items):
        results = items[0]
        return results.query.lang

    @staticmethod
    def _get_doc_lang(items):
        results = items[0]
        return results.doc_lang
