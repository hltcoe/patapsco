from .config import BaseConfig, Optional, Union
from .core import Doc
from .util.trec import parse_sgml


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
            return TrecCorpusReader(config.path, config.lang, config.encoding)


class TrecCorpusReader:
    def __init__(self, path, lang, encoding='utf8'):
        self.lang = lang
        self.docs = iter(parse_sgml(path, encoding))

    def __iter__(self):
        return self

    def __next__(self):
        doc = next(self.docs)
        return Doc(doc[0], self.lang, doc[1])
