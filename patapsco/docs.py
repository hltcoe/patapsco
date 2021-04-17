import csv
import dataclasses
import gzip
import json
import pathlib

import sqlitedict

from .config import BaseConfig, PathConfig, Union
from .error import ParseError
from .pipeline import Task
from .text import TextProcessor, StemConfig, TokenizeConfig, TruncStemConfig
from .util import trec, ComponentFactory, DataclassJSONEncoder
from .util.file import GlobFileGenerator, is_complete, touch_complete


@dataclasses.dataclass
class Doc:
    id: str
    lang: str
    text: str


class InputConfig(BaseConfig):
    """Configuration for the document corpus"""
    format: str
    lang: str
    encoding: str = "utf8"
    path: Union[str, list]


class ProcessorConfig(BaseConfig):
    """Configuration for the document processor"""
    name: str = "default"
    char_normalize: bool = True
    lowercase: bool = True
    tokenize: TokenizeConfig
    stem: Union[StemConfig, TruncStemConfig]


class DocumentsConfig(BaseConfig):
    """Document processing task configuration"""
    input: InputConfig
    process: ProcessorConfig
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
    config_class = InputConfig


class DocumentProcessorFactory(ComponentFactory):
    classes = {
        'default': 'DocumentProcessor'
    }
    config_class = ProcessorConfig


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

    def __init__(self, path):
        super().__init__()
        self.dir = pathlib.Path(path)
        self.dir.mkdir(parents=True)
        path = self.dir / 'documents.jsonl'
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
        self.file.close()
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

    def __init__(self, path, readonly=False, *args, **kwargs):
        kwargs['autocommit'] = True
        self.readonly = readonly
        self.dir = pathlib.Path(path)
        if not self.dir.exists():
            self.dir.mkdir(parents=True)
        path = str(pathlib.Path(path) / "docs.db")
        super().__init__(path, *args, **kwargs)

    def __setitem__(self, key, value):
        if self.readonly:
            return
        super().__setitem__(key, value)

    def end(self):
        touch_complete(self.dir)


class DocumentDatabaseFactory:
    @staticmethod
    def create(path):
        readonly = True if is_complete(path) else False
        return DocumentDatabase(path, readonly)


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
        self.db = db

    def process(self, doc):
        """
        Args:
            doc (Doc)

        Returns
            Doc
        """
        text = doc.text
        if self.config.char_normalize:
            text = self.normalize(text)
        if self.config.lowercase:
            text = self.lowercase_text(text)
        tokens = self.tokenize(text)
        self.db[doc.id] = doc.text
        if self.config.stem:
            tokens = self.stem(tokens)
        text = ' '.join(tokens)
        return Doc(doc.id, doc.lang, text)

    def end(self):
        self.db.end()
