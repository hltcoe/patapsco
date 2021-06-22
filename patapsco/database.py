import json
import logging
import pathlib

import sqlitedict

from .docs import Doc
from .error import BadDataError, ConfigError
from .pipeline import Task
from .util import DataclassJSONEncoder
from .util.file import is_complete

LOGGER = logging.getLogger(__name__)


def encode(doc):
    """Encode an object as json for the database"""
    return json.dumps(doc, cls=DataclassJSONEncoder)


def decode(s):
    """Decode a json string to Doc object"""
    return json.loads(s, object_hook=lambda d: Doc(**d))


class DocumentDatabase(sqlitedict.SqliteDict):
    """Key value database for documents

    Uses a dictionary interface.
    Example:
        store = DocumentDatabase('path/to/docs.db')
        store['doc_77'] = doc_object
        print(store['doc_77'])
    """

    def __init__(self, run_path, output_dir, readonly=False, *args, **kwargs):
        """
        Args:
            run_path (str): Path to run directory.
            output_dir (str): Database directory name.
            readonly (bool): Whether to support adding documents.
        """
        kwargs['autocommit'] = True
        self.readonly = readonly
        self.db_dir = pathlib.Path(run_path) / output_dir
        self.path = self.db_dir / "docs.db"
        if readonly and not self.path.exists():
            raise ConfigError(f"Document database does not exist: {self.path}")
        elif not readonly:
            self.db_dir.mkdir(parents=True, exist_ok=True)
        kwargs['encode'] = encode
        kwargs['decode'] = decode
        kwargs['tablename'] = 'patapsco'
        super().__init__(str(self.path), *args, **kwargs)

    def __setitem__(self, key, value):
        if self.readonly:
            return
        super().__setitem__(key, value)

    def __getitem__(self, key):
        try:
            return super().__getitem__(key)
        except KeyError:
            raise BadDataError(f"Unable to retrieve doc {key} from the database")


class DocumentDatabaseFactory:
    @staticmethod
    def create(run_path, output_path, readonly=False):
        db_path = pathlib.Path(run_path) / output_path
        if is_complete(db_path):
            readonly = True
        return DocumentDatabase(run_path, output_path, readonly)


class DatabaseWriter(Task):
    """Write documents to the database"""

    def __init__(self, run_path, config, artifact_config):
        """
        Args:
            run_path (str): Path of run directory.
            config (DatabaseConfig): Database config
        """
        super().__init__(run_path, artifact_config, config.output)
        self.output_path = config.output
        self.db = DocumentDatabaseFactory.create(run_path, config.output)

    def process(self, doc):
        """
        Args:
            doc (Doc)

        Returns:
            Doc
        """
        # the original_text was added by document processor for us to pull off
        db_doc = Doc(doc.id, lang=doc.lang, text=doc.original_text, date=doc.date)
        self.db[doc.id] = db_doc
        del doc.original_text
        return doc

    def reduce(self, dirs):
        LOGGER.debug("Reducing to a sqlite db from %s", ', '.join(str(x) for x in dirs))
        for base in dirs:
            path = base / 'docs.db'
            kwargs = {
                'encode': encode,
                'decode': decode,
                'tablename': 'patapsco'
            }
            db = sqlitedict.SqliteDict(str(path), **kwargs)
            for doc_id in db:
                self.db[doc_id] = db[doc_id]
