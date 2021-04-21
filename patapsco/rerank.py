import copy
import json
import pathlib
import random
import subprocess

from .config import BaseConfig, BaseUncheckedConfig, Optional, PathConfig
from .error import ConfigError, PatapscoError
from .pipeline import Task
from .results import Results, TrecResultsReader
from .util import ComponentFactory, DataclassJSONEncoder


class RerankInputConfig(BaseConfig):
    """Configuration of optional rerank inputs"""
    db: PathConfig  # if running both stages, runner will copy this from documents config
    results: Optional[PathConfig]  # set if starting stage2 at reranking


class RerankConfig(BaseUncheckedConfig):
    """Configuration for the rerank task"""
    input: RerankInputConfig
    name: str
    script: Optional[str]  # for the shell reranker
    output: PathConfig


class RerankFactory(ComponentFactory):
    classes = {
        'pacrr': 'MockReranker',
        'shell': 'ShellReranker'
    }
    config_class = RerankConfig


class Reranker(Task):
    """Rerank interface"""

    def __init__(self, config, db):
        """
        Args:
            config (RerankConfig): Configuration parameters
            db (DocumentDatabase): Document database
        """
        super().__init__()
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
        new_results = copy.copy(results.results)
        # retrieve documents and pop one to exercise db
        docs = [self.db[result.doc_id] for result in new_results]
        docs.pop()
        random.shuffle(new_results)
        return Results(results.query, 'MockReranker', new_results)


class ShellReranker(Reranker):
    """Calls a shell script for reranking

    Writes the current results to a json file for shell script to read.
    Provides the output path to script and reads the output when it is done.

    Config requirements:
     - script: preferably the full path to the script

    The script is called like so:
      /path/to/script doc_lang query_lang doc_db input_file output_file
    TODO add support for arbitrary arguments
    """

    def __init__(self, config, db):
        super().__init__(config, db)
        self.dir = pathlib.Path(config.output.path) / 'shell'
        self.dir.mkdir(parents=True, exist_ok=True)
        self.batch = 1

    def process(self, results):
        raise ConfigError("Shell reranker only runs with a batch pipeline")

    def batch_process(self, items):
        """ Call shell script to rerank the results from the retriever.

        Args:
            items (list of Results)
        """
        if not pathlib.Path(self.config.script).exists():
            raise ConfigError(f"Reranker shell script does not exist: {self.config.script}")
        query_lang = self._get_query_lang(items)
        doc_lang = self._get_doc_lang(items)
        input_path = str(self.dir / f"input_{self.batch}.json")
        output_path = str(self.dir / f"output_{self.batch}.txt")
        log_path = str(self.dir / f"log_{self.batch}.log")
        self._write_input(items, input_path)
        args = [self.config.script, doc_lang, query_lang, self.db.path, input_path, output_path]
        try:
            record = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True)
            self._write_log(log_path, record.args, record.stdout)
        except subprocess.CalledProcessError as e:
            self._write_log(log_path, e.cmd, e.output)
            raise PatapscoError(e)

        self.batch += 1
        return self._read_output(output_path, query_lang)

    @staticmethod
    def _write_input(items, input_path):
        with open(input_path, 'w') as fp:
            json.dump(items, fp, cls=DataclassJSONEncoder)

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

    def _get_doc_lang(self, items):
        results = items[0]
        doc = self.db[results.results[0].doc_id]
        return doc.lang
