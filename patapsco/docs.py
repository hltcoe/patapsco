import csv
import dataclasses
import gzip
import json
import pathlib

import sqlitedict

from .config import BaseConfig, ConfigService, PathConfig, Union
from .error import ConfigError, ParseError
from .pipeline import Task
from .text import Splitter, TextProcessor, TextProcessorConfig
from .util import trec, ComponentFactory, DataclassJSONEncoder
from .util.file import GlobFileGenerator, is_complete, touch_complete


@dataclasses.dataclass
class Doc:
    id: str
    lang: str
    text: str


class DocumentsInputConfig(BaseConfig):
    """Configuration for the document corpus"""
    format: str
    lang: str
    encoding: str = "utf8"
    path: Union[str, list]


class DocumentsConfig(BaseConfig):
    """Document processing task configuration"""
    input: DocumentsInputConfig
    process: TextProcessorConfig
    output: Union[bool, PathConfig]
    db: PathConfig


class DocumentReaderFactory(ComponentFactory):
    classes = {
        'sgml': 'SgmlDocumentReader',
        'json': 'Tc4JsonDocumentReader',
        'jsonl': 'Tc4JsonDocumentReader',
        'msmarco': 'TsvDocumentReader',
        'clef0809': 'HamshahriDocumentReader'
    }
    config_class = DocumentsInputConfig


class SgmlDocumentReader:
    """Iterator that reads TREC sgml documents"""

    def __init__(self, config):
        self.lang = config.lang
        self.docs = GlobFileGenerator(config.path, trec.parse_sgml_documents, config.encoding)

    def __iter__(self):
        return self

    def __next__(self):
        doc = next(self.docs)
        return Doc(doc[0], self.lang, doc[1])


class Tc4JsonDocumentReader:
    """Read JSONL documents to start a pipeline"""

    def __init__(self, config):
        self.lang = config.lang
        self.docs = GlobFileGenerator(config.path, self.parse, config.encoding)

    def __iter__(self):
        return self

    def __next__(self):
        doc = next(self.docs)
        return Doc(doc[0], self.lang, doc[1])

    @staticmethod
    def parse(path, encoding='utf8'):
        open_func = gzip.open if path.endswith('.gz') else open
        with open_func(path, 'rt', encoding=encoding) as fp:
            for line in fp:
                try:
                    data = json.loads(line.strip())
                except json.decoder.JSONDecodeError as e:
                    raise ParseError(f"Problem parsing json from {path}: {e}")
                try:
                    yield data['id'], ' '.join([data['title'].strip(), data['text'].strip()])
                except KeyError as e:
                    raise ParseError(f"Missing field {e} in json docs element: {data}")


class TsvDocumentReader:
    """Iterator that reads TSV documents like from MSMARCO"""

    def __init__(self, config):
        self.lang = config.lang
        self.docs = GlobFileGenerator(config.path, self.parse, config.encoding)

    def __iter__(self):
        return self

    def __next__(self):
        doc = next(self.docs)
        return Doc(doc[0], self.lang, doc[1])

    @staticmethod
    def parse(path, encoding='utf8'):
        open_func = gzip.open if path.endswith('.gz') else open
        with open_func(path, 'rt', encoding=encoding) as fp:
            reader = csv.reader(fp, delimiter='\t')
            for line in reader:
                yield line[0], line[1].strip()


class HamshahriDocumentReader:
    """Iterator that reads CLEF Farsi documents"""

    def __init__(self, config):
        self.lang = config.lang
        self.docs = GlobFileGenerator(config.path, trec.parse_hamshahri_documents, config.encoding)

    def __iter__(self):
        return self

    def __next__(self):
        doc = next(self.docs)
        return Doc(doc[0], self.lang, doc[1])


class DocWriter(Task):
    """Write documents to a json file"""

    def __init__(self, config, artifact_config):
        super().__init__()
        self.dir = pathlib.Path(config.output.path)
        self.dir.mkdir(parents=True)
        path = self.dir / 'documents.jsonl'
        self.file = open(path, 'w')
        self.config = artifact_config
        self.config_path = self.dir / 'config.yml'

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
        self.file.close()
        if self.config:
            ConfigService.write_config_file(self.config_path, self.config)
        touch_complete(self.dir)


class DocReader:
    """Iterator over documents written by DocWriter"""

    def __init__(self, path):
        path = pathlib.Path(path) / 'documents.jsonl'
        self.file = open(path, 'r')

    def __iter__(self):
        return self

    def __next__(self):
        line = self.file.readline()
        if not line:
            self.file.close()
            raise StopIteration
        data = json.loads(line)
        return Doc(**data)


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
        self.dir = pathlib.Path(path)
        self.path = self.dir / "docs.db"
        if readonly and not self.path.exists():
            raise ConfigError(f"Document database does not exist: {self.path}")
        if not self.dir.exists():
            self.dir.mkdir(parents=True)
        self.config = config
        self.config_path = self.dir / 'config.yml'
        super().__init__(str(self.path), *args, **kwargs)

    def __setitem__(self, key, value):
        if self.readonly:
            return
        super().__setitem__(key, value)

    def end(self):
        if not self.readonly:
            ConfigService.write_config_file(self.config_path, self.config)
            touch_complete(self.dir)


class DocumentDatabaseFactory:
    @staticmethod
    def create(path, config=None, readonly=False):
        if is_complete(path):
            readonly = True
            config = ConfigService().read_config_file(pathlib.Path(path) / 'config.yml')
        return DocumentDatabase(path, config, readonly)


class DocumentProcessor(Task, TextProcessor):
    """Document Preprocessing"""

    def __init__(self, config, db):
        """
        Args:
            config (ProcessorConfig)
            db (DocumentDatabase): Document db for later retrieval
        """
        Task.__init__(self)
        TextProcessor.__init__(self, config)
        self.splitter = Splitter(config.splits)
        self.db = db

    def process(self, doc):
        """
        Args:
            doc (Doc)

        Returns
            Doc
        """
        if not self.initialized:
            self.initialize(doc.lang)

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

    @property
    def name(self):
        if self.splitter:
            return f"{super()} | Splitter"
        else:
            return str(super())
