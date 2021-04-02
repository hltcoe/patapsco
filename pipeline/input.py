import collections
import csv

from .config import BaseConfig, Optional, Union
from .doc import Doc
from .query import Topic
from .error import ConfigError
from .util.trec import parse_sgml, parse_topics


class InputDocumentsConfig(BaseConfig):
    lang: str
    encoding: str = "utf8"
    format: str
    path: str


class DocumentReaderFactory:
    @classmethod
    def create(cls, config):
        config = InputDocumentsConfig(**config)
        if config.format == "trec":
            return TrecDocumentReader(config.path, config.lang, config.encoding)
        else:
            raise ConfigError(f"Unknown document format: {config.format}")


class TrecDocumentReader:
    def __init__(self, path, lang, encoding='utf8'):
        self.lang = lang
        self.docs = iter(parse_sgml(path, encoding))

    def __iter__(self):
        return self

    def __next__(self):
        doc = next(self.docs)
        return Doc(doc[0], self.lang, doc[1])


class DocumentStore:
    def __getitem__(self, doc_id):
        return "Hello, world"


class InputTopicsConfig(BaseConfig):
    lang: str
    encoding: str = "utf8"
    format: str
    path: str


class TopicReaderFactory:
    @classmethod
    def create(cls, config):
        config = InputTopicsConfig(**config)
        if config.format == "trec":
            return TrecTopicReader(config.path, config.lang, config.encoding)
        else:
            raise ConfigError(f"Unknown topic format: {config.format}")


class TrecTopicReader:
    def __init__(self, path, lang, encoding='utf8'):
        self.lang = lang
        self.topics = parse_topics(path, 'EN-', encoding)

    def __iter__(self):
        return self

    def __next__(self):
        topic = next(self.topics)
        return Topic(topic[0], self.lang, topic[1], topic[2], topic[3])


class InputQrelsConfig(BaseConfig):
    format: str
    path: str


class QrelsReaderFactory:
    @classmethod
    def create(cls, config):
        config = InputQrelsConfig(**config)
        if config.format == "trec":
            return TrecQrelsReader(config.path)
        else:
            raise ConfigError(f"Unknown qrels format: {config.format}")


class TrecQrelsReader:
    def __init__(self, path):
        self.path = path

    def read(self):
        with open(self.path, 'r') as fp:
            reader = csv.reader(fp, delimiter=' ')
            qrels = collections.defaultdict(dict)
            for row in reader:
                qrels[row[0]][row[2]] = int(row[3])
            return qrels
