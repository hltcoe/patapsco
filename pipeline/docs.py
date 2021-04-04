import collections

from .config import BaseConfig, Union
from .text import TextProcessor, StemConfig, TokenizeConfig, TruncStemConfig
from .util import trec, ComponentFactory

Doc = collections.namedtuple('Doc', ('id', 'lang', 'text'))


class InputConfig(BaseConfig):
    name: str
    lang: str
    encoding: str = "utf8"
    path: str


class ProcessorConfig(BaseConfig):
    name: str = "default"
    utf8_normalize: bool = True
    lowercase: bool = True
    tokenize: TokenizeConfig
    stem: Union[StemConfig, TruncStemConfig]


class DocumentReaderFactory(ComponentFactory):
    classes = {
        'trec': 'TrecDocumentReader'
    }
    config_class = InputConfig


class DocumentProcessorFactory(ComponentFactory):
    classes = {
        'default': 'DocumentProcessor'
    }
    config_class = ProcessorConfig


class TrecDocumentReader:
    def __init__(self, config):
        self.lang = config.lang
        self.docs = iter(trec.parse_sgml(config.path, config.encoding))

    def __iter__(self):
        return self

    def __next__(self):
        doc = next(self.docs)
        return Doc(doc[0], self.lang, doc[1])


class DocumentProcessor(TextProcessor):
    """Document Preprocessing"""
    def __init__(self, config):
        """
        Args:
            config (ProcessorConfig)
        """
        super().__init__(config)

    def run(self, doc):
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
        if self.config.stem:
            tokens = self.stem(tokens)
        text = ' '.join(tokens)
        return Doc(doc.id, doc.lang, text)
