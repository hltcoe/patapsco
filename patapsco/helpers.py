import pathlib

from .config import ConfigService
from .error import ConfigError
from .schema import RunnerConfig, Tasks


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


class ArtifactHelper:
    """Utilities for working with artifacts"""

    def __init__(self):
        self.excludes = {
            Tasks.DOCUMENTS: [Tasks.INDEX, Tasks.TOPICS, Tasks.QUERIES, Tasks.RETRIEVE, Tasks.RERANK, Tasks.SCORE],
            Tasks.INDEX: [Tasks.TOPICS, Tasks.QUERIES, Tasks.RETRIEVE, Tasks.RERANK, Tasks.SCORE],
            Tasks.TOPICS: [Tasks.DOCUMENTS, Tasks.INDEX, Tasks.QUERIES, Tasks.RETRIEVE, Tasks.RERANK, Tasks.SCORE],
            Tasks.QUERIES: [Tasks.DOCUMENTS, Tasks.INDEX, Tasks.RETRIEVE, Tasks.RERANK, Tasks.SCORE],
            Tasks.RETRIEVE: [Tasks.RERANK, Tasks.SCORE],
            Tasks.RERANK: [Tasks.SCORE]
        }

    def get_config(self, config, task):
        """This excludes the parts of the configuration that were not used to create the artifact."""
        return config.copy(exclude=set(self.excludes[task]), deep=True)

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
        for task in Tasks:
            if getattr(artifact_config, task):
                if not getattr(config, task):
                    setattr(config, task, getattr(artifact_config, task))
