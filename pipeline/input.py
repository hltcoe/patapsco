from .config import BaseConfig, Optional, Union
from .core import Doc, Topic
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


class TrecDocumentReader:
    def __init__(self, path, lang, encoding='utf8'):
        self.lang = lang
        self.docs = iter(parse_sgml(path, encoding))

    def __iter__(self):
        return self

    def __next__(self):
        doc = next(self.docs)
        return Doc(doc[0], self.lang, doc[1])


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


class TrecTopicReader:
    def __init__(self, path, lang, encoding='utf8'):
        self.lang = lang
        self.topics = parse_topics(path, 'EN-', encoding)

    def __iter__(self):
        return self

    def __next__(self):
        topic = next(self.topics)
        return Topic(topic[0], self.lang, topic[1], topic[2], topic[3])
