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
from .text import Splitter, TextProcessor
from .util import trec, DataclassJSONEncoder, InputIterator, ReaderFactory
from .util.file import count_lines, count_lines_with, path_append, is_complete, touch_complete

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class Doc:
    id: str
    lang: str
    text: str


class DocumentReaderFactory(ReaderFactory):
    classes = {
        'sgml': 'SgmlDocumentReader',
        'json': 'Tc4JsonDocumentReader',
        'jsonl': 'Tc4JsonDocumentReader',
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
        self.docs_iter = iter(trec.parse_sgml_documents(path, encoding))

    def __iter__(self):
        return self

    def __next__(self):
        doc = next(self.docs_iter)
        return Doc(doc[0], self.lang, doc[1])

    def __len__(self):
        return count_lines_with('<DOC>', self.path, self.encoding)


class Tc4JsonDocumentReader(InputIterator):
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
            return Doc(data['id'], self.lang, ' '.join([data['title'].strip(), data['text'].strip()]))
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
            return Doc(row[0], self.lang, row[1])
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
        self.docs_iter = iter(trec.parse_hamshahri_documents(path, encoding))

    def __iter__(self):
        return self

    def __next__(self):
        doc = next(self.docs_iter)
        return Doc(doc[0], self.lang, doc[1])

    def __len__(self):
        return count_lines_with('.DID', self.path, self.encoding)


class DocWriter(Task):
    """Write documents to a json file using internal format"""

    def __init__(self, config, artifact_config):
        super().__init__(artifact_config, config.output.path)
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
        store = DocumentDatabase('docs.sqlite')
        store['doc_77'] = 'some text'
        print(store['doc_77'])
    """

    def __init__(self, path, config, readonly=False, *args, **kwargs):
        kwargs['autocommit'] = True
        self.readonly = readonly
        self.base = pathlib.Path(path)
        self.path = self.base / "docs.db"
        if readonly and not self.path.exists():
            raise ConfigError(f"Document database does not exist: {self.path}")
        if not self.base.exists():
            self.base.mkdir(parents=True)
        self.config = config
        self.config_path = self.base / 'config.yml'
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

    def end(self):
        if not self.readonly:
            ConfigService.write_config_file(self.config_path, self.config)
            touch_complete(self.base)

    def reduce(self):
        dirs = sorted(list(self.base.glob('part*')))
        for base in dirs:
            path = path_append(base, 'docs.db')
            db = sqlitedict.SqliteDict(str(path))
            for doc_id in db:
                self[doc_id] = db[doc_id]


class DocumentDatabaseFactory:
    @staticmethod
    def create(path, config=None, readonly=False):
        if is_complete(path):
            readonly = True
            config = ConfigService().read_config_file(pathlib.Path(path) / 'config.yml')
        return DocumentDatabase(path, config, readonly)


class DocumentProcessor(Task, TextProcessor):
    """Document Preprocessing"""

    def __init__(self, config, lang, db):
        """
        Args:
            config (TextProcessorConfig)
            lang (str): Language code for the documents.
            db (DocumentDatabase): Document db for later retrieval.
        """
        Task.__init__(self)
        TextProcessor.__init__(self, config, lang)
        self.splitter = Splitter(config.splits)
        self.db = db

    def process(self, doc):
        """
        Args:
            doc (Doc)

        Returns
            Doc
        """
        self.splitter.reset()
        text = doc.text
        if self.config.normalize:
            text = self.normalize(text)
        tokens = self.tokenize(text)
        self.splitter.add('tokenize', Doc(doc.id, doc.lang, ' '.join(tokens)))
        if self.config.lowercase:
            tokens = self.lowercase(tokens)
        self.splitter.add('lowercase', Doc(doc.id, doc.lang, ' '.join(tokens)))
        self.db[doc.id] = ' '.join(tokens)
        if self.config.stopwords:
            tokens = self.remove_stop_words(tokens, not self.config.lowercase)
        self.splitter.add('stopwords', Doc(doc.id, doc.lang, ' '.join(tokens)))
        if self.config.stem:
            tokens = self.stem(tokens)
        self.splitter.add('stem', Doc(doc.id, doc.lang, ' '.join(tokens)))

        if self.splitter:
            return self.splitter.get()
        else:
            return Doc(doc.id, doc.lang, ' '.join(tokens))

    def end(self):
        self.db.end()

    def run_reduce(self):
        self.db.reduce()

    @property
    def name(self):
        if self.splitter:
            return f"{super()} | Splitter"
        else:
            return str(super())
