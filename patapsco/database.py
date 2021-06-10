import json
import pathlib

import sqlitedict

from .config import ConfigService
from .docs import Doc
from .error import BadDataError, ConfigError
from .util import DataclassJSONEncoder
from .util.file import is_complete, touch_complete


class DocumentDatabase(sqlitedict.SqliteDict):
    """Key value database for documents

    Uses a dictionary interface.
    Example:
        store = DocumentDatabase('path/to/docs.db')
        store['doc_77'] = doc_object
        print(store['doc_77'])
    """

    def __init__(self, run_path, output_path, artifact_config, readonly=False, *args, **kwargs):
        """
        Args:
            run_path (str): Path to run directory.
            output_path (str): Database directory.
            artifact_config (RunnerConfig): Config that resulted in this database.
            readonly (bool): Whether to support adding documents.
        """
        kwargs['autocommit'] = True
        self.readonly = readonly
        self.run_path = pathlib.Path(run_path)
        self.output_path = output_path
        self.base = self.run_path / output_path
        self.db_path = self.base / "docs.db"
        if readonly and not self.db_path.exists():
            raise ConfigError(f"Document database does not exist: {self.db_path}")
        if not self.base.exists():
            self.base.mkdir(parents=True)
        self.artifact_config = artifact_config
        self.config_path = self.base / 'config.yml'
        super().__init__(str(self.db_path), *args, **kwargs)

    def __setitem__(self, key, value):
        if self.readonly:
            return
        super().__setitem__(key, json.dumps(value, cls=DataclassJSONEncoder))

    def __getitem__(self, key):
        try:
            return json.loads(super().__getitem__(key), object_hook=lambda d: Doc(**d))
        except KeyError:
            raise BadDataError(f"Unable to retrieve doc {key} from the database")

    def end(self):
        if not self.readonly:
            ConfigService.write_config_file(self.config_path, self.artifact_config)
            touch_complete(self.base)

    def reduce(self):
        # because this is not a task, we need to do the glob ourselves
        dirs = sorted(list(self.run_path.glob('part*')))
        for base in dirs:
            path = base / self.output_path / 'docs.db'
            db = sqlitedict.SqliteDict(str(path))
            for doc_id in db:
                self[doc_id] = db[doc_id]


class DocumentDatabaseFactory:
    @staticmethod
    def create(run_path, output_path, config=None, readonly=False):
        db_path = pathlib.Path(run_path) / output_path
        if is_complete(db_path):
            readonly = True
            config = ConfigService().read_config_file(db_path / 'config.yml')
        return DocumentDatabase(run_path, output_path, config, readonly)
