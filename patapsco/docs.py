import collections
import csv
import dataclasses
import gzip
import json
import logging
import pathlib

import sqlitedict

from .config import ConfigService
from .error import BadDataError, ConfigError, ParseError
from .pipeline import Task
from .schema import DocumentsInputConfig
from .text import TextProcessor
from .util import DataclassJSONEncoder, InputIterator, ReaderFactory
from .util.file import count_lines, count_lines_with, delete_dir, path_append, is_complete, touch_complete
from .util.formats import parse_sgml_documents, parse_hamshahri_documents
from .util.normalize import compare_strings

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class Doc:
    id: str
    lang: str
    text: str
    date: str


class DocumentReaderFactory(ReaderFactory):
    classes = {
        'sgml': 'SgmlDocumentReader',
        'json': 'Hc4JsonDocumentReader',
        'jsonl': 'Hc4JsonDocumentReader',
        'msmarco': 'TsvDocumentReader',
        'clef0809': 'HamshahriDocumentReader'
    }
    config_class = DocumentsInputConfig
    name = "input document type"


class SgmlDocumentReader(InputIterator):
    """Iterator that reads a TREC sgml document"""

    def __init__(self, path, encoding, lang, **kwargs):
        self.path = path
        self.encoding = encoding
        self.lang = lang
        self.docs_iter = iter(parse_sgml_documents(path, encoding))

    def __iter__(self):
        return self

    def __next__(self):
        doc = next(self.docs_iter)
        return Doc(doc[0], self.lang, doc[1], None)

    def __len__(self):
        return count_lines_with('<DOC>', self.path, self.encoding)


class Hc4JsonDocumentReader(InputIterator):
    """Read documents from a JSONL file to start a pipeline"""

    def __init__(self, path, encoding, lang, **kwargs):
        """
        Args:
            path (str): Path to file to parse
            encoding (str): Encoding of file
            lang (str): Language of documents in file
        """
        self.path = path
        self.encoding = encoding
        self.lang = lang
        open_func = gzip.open if path.endswith('.gz') else open
        self.fp = open_func(path, 'rt', encoding=encoding)
        self.count = 0

    def __iter__(self):
        return self

    def __next__(self):
        self.count += 1
        line = self.fp.readline()
        if not line:
            self.fp.close()
            raise StopIteration()
        try:
            data = json.loads(line.strip())
            return Doc(data['id'], self.lang, ' '.join([data['title'].strip(), data['text'].strip()]), data['date'])
        except json.decoder.JSONDecodeError as e:
            raise ParseError(f"Problem parsing json from {self.path} on line {self.count}: {e}")
        except KeyError as e:
            raise ParseError(f"Missing field {e} in json element in {self.path} on line {self.count}")

    def __len__(self):
        return count_lines(self.path, self.encoding)


class TsvDocumentReader(InputIterator):
    """Iterator that reads TSV documents from MSMARCO Passages"""

    def __init__(self, path, encoding, lang, **kwargs):
        self.path = path
        self.encoding = encoding
        self.lang = lang
        open_func = gzip.open if path.endswith('.gz') else open
        self.fp = open_func(path, 'rt', encoding=encoding)
        self.reader = csv.reader(self.fp, delimiter='\t')

    def __iter__(self):
        return self

    def __next__(self):
        try:
            row = next(self.reader)
            return Doc(row[0], self.lang, row[1], None)
        except StopIteration:
            self.fp.close()
            raise

    def __len__(self):
        return count_lines(self.path, self.encoding)


class HamshahriDocumentReader(InputIterator):
    """Iterator that reads CLEF Farsi documents"""

    def __init__(self, path, encoding, lang, **kwargs):
        self.path = path
        self.encoding = encoding
        self.lang = lang
        self.docs_iter = iter(parse_hamshahri_documents(path, encoding))

    def __iter__(self):
        return self

    def __next__(self):
        doc = next(self.docs_iter)
        return Doc(doc[0], self.lang, doc[1])

    def __len__(self):
        return count_lines_with('.DID', self.path, self.encoding)


class DocWriter(Task):
    """Write documents to a json file using internal format"""

    def __init__(self, run_path, config, artifact_config):
        super().__init__(run_path, artifact_config, config.output)
        path = self.base / 'documents.jsonl'
        self.file = open(path, 'w')

    def process(self, doc):
        """
        Args:
            doc (Doc)

        Returns:
            Doc
        """
        self.file.write(json.dumps(doc, cls=DataclassJSONEncoder) + "\n")
        return doc

    def end(self):
        super().end()
        self.file.close()

    def reduce(self, dirs):
        for base in dirs:
            path = path_append(base, 'documents.jsonl')
            with open(path) as fp:
                for line in fp:
                    self.file.write(line)


class DocReader(InputIterator):
    """Iterator over documents written by DocWriter"""

    def __init__(self, path):
        self.path = pathlib.Path(path)
        if self.path.is_dir():
            self.path = self.path / 'documents.jsonl'
        self.file = open(self.path, 'r')

    def __iter__(self):
        return self

    def __next__(self):
        line = self.file.readline()
        if not line:
            self.file.close()
            raise StopIteration
        data = json.loads(line)
        return Doc(**data)

    def __len__(self):
        return count_lines(self.path)


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
        super().__setitem__(key, json.dumps(value))

    def __getitem__(self, key):
        try:
            return json.loads(super().__getitem__(key))
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


class DocumentProcessor(TextProcessor):
    """Document Preprocessing"""

    def __init__(self, run_path, config, lang, db):
        """
        Args:
            run_path (str): Root directory of the run.
            config (DocumentsConfig)
            lang (str): Language code for the documents.
            db (DocumentDatabase): Document db for later retrieval.
        """
        super().__init__(run_path, config.process, lang)
        self.db = db
        self.save_report = config.process.normalize.report
        self.diffs = collections.Counter()

    def process(self, doc):
        """
        Args:
            doc (Doc)

        Returns
            Doc
        """
        text = original_text = doc.text
        text = self.pre_normalize(text)
        if self.save_report:
            self.diffs += compare_strings(original_text, text)

        tokens = self.tokenize(text)
        if self.run_lowercase:
            tokens = self.lowercase(tokens)
        self.db[doc.id] = ' '.join(tokens)
        stopword_indices = self.identify_stop_words(tokens, self.run_lowercase)
        tokens = self.stem(tokens)
        tokens = self.remove_stop_words(tokens, stopword_indices)
        text = self.post_normalize(' '.join(tokens))
        return Doc(doc.id, doc.lang, text, None)

    def end(self):
        self.db.end()
        if self.save_report:
            self._save_report()

    def _save_report(self):
        with open(self.run_path / 'normalize_report.txt', 'w') as fp:
            for change, count in self.diffs.most_common(len(self.diffs)):
                if "\n" not in change:  # skip newline removal
                    fp.write(f"{repr(change)}\t{count}\n")

    def run_reduce(self):
        self.db.reduce()
