import pathlib

from .config import ConfigService
from .error import ConfigError
from .schema import PathConfig, RerankInputConfig, RetrieveInputConfig, RunnerConfig, Tasks


class ConfigHelper:
    """Utility methods for working with configuration"""

    @classmethod
    def load(cls, config_filename, overrides):
        """Load config and perform some basic checks and updates

        1. sets the run directory based on run name if not already set
        2. sets the output directory names from defaults if not already set
        3. sets the retriever's index path based on the index task if not already set
        4. sets the reranker's db path based on the database section if not already set
        """
        config_service = ConfigService(overrides)
        try:
            conf_dict = config_service.read_config_file(config_filename)
        except FileNotFoundError as error:
            raise ConfigError(error)
        cls._validate(conf_dict)
        cls._set_run_path(conf_dict)
        conf = config_service.create_config_object(RunnerConfig, conf_dict)
        cls._set_output_paths(conf)
        cls._make_input_paths_absolute(conf)
        cls._set_retrieve_input_path(conf)
        cls._set_rerank_db_path(conf)
        cls._set_progress_intervals(conf)
        return conf

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
        'documents': 'docs',
        'index': 'index',
        'topics': 'raw_queries',
        'queries': 'processed_queries',
        'retrieve': 'retrieve',
        'rerank': 'rerank',
        'database': 'database'
    }

    @classmethod
    def _set_output_paths(cls, conf):
        # set output path for components from defaults
        for task in cls.output_defaults.keys():
            if conf.get(task) and conf.get(task).get('output') is True:
                conf.get(task).set('output', cls.output_defaults[task])

    @classmethod
    def _make_input_paths_absolute(cls, conf):
        # if user configured any input paths, make them absolute
        # also resolve symbolic links
        cls._make_absolute(conf, 'documents.input')
        cls._make_absolute(conf, 'index.input.documents')
        cls._make_absolute(conf, 'topics.input')
        cls._make_absolute(conf, 'queries.input')
        cls._make_absolute(conf, 'retrieve.input.index')
        cls._make_absolute(conf, 'retrieve.input.queries')
        cls._make_absolute(conf, 'rerank.input.database')
        cls._make_absolute(conf, 'rerank.input.results')
        cls._make_absolute(conf, 'score.input')

    @staticmethod
    def _make_absolute(conf, attribute):
        obj = conf
        fields = attribute.split('.')
        try:
            while fields:
                field = fields.pop(0)
                obj = getattr(obj, field)
            if isinstance(obj.path, list):
                obj.path = [str(pathlib.Path(path).absolute()) for path in obj.path]
            elif isinstance(obj.path, dict):
                obj.path = {key: str(pathlib.Path(path).absolute()) for key, path in obj.path.items()}
            else:
                # make path absolute if not relative to root run directory
                path = pathlib.Path(obj.path).resolve().absolute()
                if path.exists():
                    obj.path = str(path)
        except AttributeError:
            pass

    @staticmethod
    def _set_retrieve_input_path(conf):
        # if index location for retrieve is not set, we grab it from index config
        if conf.retrieve:
            if not conf.retrieve.input or not conf.retrieve.input.index:
                if conf.index and conf.index.output:
                    if not conf.retrieve.input:
                        conf.retrieve.input = RetrieveInputConfig(index=PathConfig(path=conf.index.output))
                    else:
                        conf.retrieve.input.index = PathConfig(path=conf.index.output)
                else:
                    raise ConfigError("retrieve.input.index.path needs to be set")

    @staticmethod
    def _set_rerank_db_path(conf):
        # if database path for rerank is not set, we grab it from documents config
        if conf.rerank:
            if not conf.rerank.input or not conf.rerank.input.database:
                if conf.database and conf.database.output:
                    if not conf.rerank.input:
                        conf.rerank.input = RerankInputConfig(database=PathConfig(path=conf.database.output))
                    else:
                        conf.rerank.input.database = PathConfig(path=conf.database.output)
                else:
                    raise ConfigError("rerank.input.database.path needs to be set")

    @staticmethod
    def _set_progress_intervals(conf):
        if conf.run.stage1:
            if not conf.run.stage1.progress_interval:
                conf.run.stage1.progress_interval = 10000
        if conf.run.stage2:
            if not conf.run.stage2.progress_interval:
                conf.run.stage2.progress_interval = 10


class ArtifactHelper:
    """Utilities for working with artifacts"""

    def __init__(self):
        self.excludes = {
            Tasks.DOCUMENTS: [Tasks.DATABASE, Tasks.INDEX, Tasks.TOPICS, Tasks.QUERIES, Tasks.RETRIEVE, Tasks.RERANK],
            Tasks.DATABASE: [Tasks.INDEX, Tasks.TOPICS, Tasks.QUERIES, Tasks.RETRIEVE, Tasks.RERANK],
            Tasks.INDEX: [Tasks.DATABASE, Tasks.TOPICS, Tasks.QUERIES, Tasks.RETRIEVE, Tasks.RERANK],
            Tasks.TOPICS: [Tasks.DOCUMENTS, Tasks.DATABASE, Tasks.INDEX, Tasks.QUERIES, Tasks.RETRIEVE, Tasks.RERANK],
            Tasks.QUERIES: [Tasks.DOCUMENTS, Tasks.DATABASE, Tasks.INDEX, Tasks.RETRIEVE, Tasks.RERANK],
            Tasks.RETRIEVE: [Tasks.DATABASE, Tasks.RERANK],
            Tasks.RERANK: []
        }

    def get_config(self, config, task):
        """This excludes the parts of the configuration that were not used to create the artifact."""
        excludes = set(self.excludes[task]) | {'score'}
        return config.copy(exclude=excludes, deep=True)

    def combine(self, config, path, required=True):
        """Loads an artifact configuration and combines it with the base config"""
        path = pathlib.Path(path)
        if path.is_dir():
            path = path / 'config.yml'
        else:
            path = path.parent / 'config.yml'
        try:
            artifact_config_dict = ConfigService().read_config_file(path)
        except FileNotFoundError:
            if required:
                raise ConfigError(f"Unable to load artifact config {path}")
            else:
                return
        artifact_config = RunnerConfig(**artifact_config_dict)
        for task in Tasks:
            if getattr(artifact_config, task):
                if not getattr(config, task):
                    setattr(config, task, getattr(artifact_config, task))
