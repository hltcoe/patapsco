import collections

from .config import BaseConfig, Union
from .error import ConfigError
from .text import TextProcessor, StemConfig, TokenizeConfig, TruncStemConfig

Doc = collections.namedtuple('Doc', ('id', 'lang', 'text'))


class DocProcessorConfig(BaseConfig):
    name: str = "default"
    utf8_normalize: bool = True
    lowercase: bool = True
    output: str
    overwrite: bool = False
    tokenize: TokenizeConfig
    stem: Union[StemConfig, TruncStemConfig]


class DocumentProcessorFactory:
    classes = {
        'default': 'DocumentProcessor'
    }

    @classmethod
    def create(cls, config):
        """
        Args:
            config (dict)
        """
        config = DocProcessorConfig(**config)
        try:
            class_name = cls.classes[config.name]
        except KeyError:
            raise ConfigError(f"Unknown document processor: {config.name}")
        try:
            class_ = globals()[class_name]
        except KeyError:
            raise RuntimeError(f"Cannot find {class_name} in {cls.__name__}")
        return class_(config)


class DocumentProcessor(TextProcessor):
    """Document Preprocessing"""
    def __init__(self, config):
        """
        Args:
            config (DocProcessorConfig)
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
