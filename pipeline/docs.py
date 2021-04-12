import collections
import csv
import gzip
import json
import pathlib

import sqlitedict

from .config import BaseConfig, Union
from .error import ParseError
from .pipeline import Task
from .text import TextProcessor, StemConfig, TokenizeConfig, TruncStemConfig
from .util import trec, ComponentFactory
from .util.file import GlobFileGenerator

Doc = collections.namedtuple('Doc', ('id', 'lang', 'text'))


class InputConfig(BaseConfig):
    """Configuration for the document corpus"""
    name: str
    lang: str
    encoding: str = "utf8"
    path: Union[str, list]


class ProcessorConfig(BaseConfig):
    """Configuration for the document processor"""
    name: str = "default"
    utf8_normalize: bool = True
    lowercase: bool = True
    tokenize: TokenizeConfig
    stem: Union[StemConfig, TruncStemConfig]


class DocumentReaderFactory(ComponentFactory):
    classes = {
        'sgml': 'SgmlDocumentReader',
        'json': 'JsonDocumentReader',
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


class JsonDocumentReader:
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
    """Write documents to files

    This is not very efficient and should be rewritten if we want to use in production.
    This will create one file per document in a single directory.
    """

    def __init__(self, path):
        super().__init__()
        self.dir = pathlib.Path(path)
        self.dir.mkdir(parents=True)

    def process(self, doc):
        """
        Args:
            doc (Doc)

        Returns:
            Doc
        """
        path = self.dir / doc.id
        with open(path, 'w') as fp:
            fp.write(doc.text)
        return doc


class DocumentStore(sqlitedict.SqliteDict):
    """Key value store for documents

    Uses a dictionary interface.
    Example:
        store = DocumentStore('docs.sqlite')
        store['doc_77'] = 'some text'
        print(store['doc_77'])
    """

    def __init__(self, path, *args, **kwargs):
        kwargs['autocommit'] = True
        self.dir = pathlib.Path(path)
        self.dir.mkdir(parents=True)
        path = str(pathlib.Path(path) / "docs.db")
        super().__init__(path, *args, **kwargs)


class DocumentProcessor(Task, TextProcessor):
    """Document Preprocessing"""

    def __init__(self, config, store):
        """
        Args:
            config (ProcessorConfig)
            store (DocumentStore): Document storage for later retrieval
        """
        Task.__init__(self)
        TextProcessor.__init__(self, config)
        self.store = store

    def process(self, doc):
        """
        Args:
            doc (Doc)

        Returns
            Doc
        """
        text = doc.text
        if self.config.utf8_normalize:
            text = self.normalize(text)
        if self.config.lowercase:
            text = self.lowercase_text(text)
        tokens = self.tokenize(text)
        self.store[doc.id] = doc.text
        if self.config.stem:
            tokens = self.stem(tokens)
        text = ' '.join(tokens)
        return Doc(doc.id, doc.lang, text)
