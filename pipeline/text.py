from .config import BaseConfig, Optional, Union
from .core import Doc
from .error import ConfigError


class Tokenizer:
    def __init__(self, config):
        self.config = config

    """Tokenizer interface"""
    def tokenize(self, text):
        """Tokenize text

        Args:
            text (str)

        Returns:
            list: A list of strings
        """
        pass


class WhiteSpaceTokenizer(Tokenizer):
    def tokenize(self, text):
        return text.split()


class TokenizeConfig(BaseConfig):
    name: str


class TokenizerFactory:
    classes = {
        'whitespace': WhiteSpaceTokenizer
    }

    @classmethod
    def create(cls, config):
        if config.name not in cls.classes:
            raise ConfigError(f"Unknown tokenizer: {config.name}")
        return cls.classes[config.name](config)


class Stemmer:
    """Stemmer interface"""
    def __init__(self, config):
        self.config = config

    def stem(self, tokens):
        """Stem the tokens

        Args:
            tokens (list of str)

        Returns:
            list: A list of strings
        """
        pass


class TruncatingStemmer(Stemmer):
    def stem(self, tokens):
        length = self.config.length
        return [x[:length] for x in tokens]


class StemConfig(BaseConfig):
    name: str


class TruncStemConfig(BaseConfig):
    name: str
    length: int


class StemmerFactory:
    classes = {
        'trunc': TruncatingStemmer
    }

    @classmethod
    def create(cls, config):
        if config.name not in cls.classes:
            raise ConfigError(f"Unknown stemmer: {config.name}")
        return cls.classes[config.name](config)


class Normalizer:
    def normalize(self, text):
        return text


class DocProcessorConfig(BaseConfig):
    utf8_normalize: bool = True
    lowercase: bool = True
    output: str
    overwrite: bool = False
    tokenize: TokenizeConfig
    stem: Union[StemConfig, TruncStemConfig]


class TextProcessor:
    def __init__(self, config):
        config = DocProcessorConfig(**config)
        self.config = config
        self.normalizer = Normalizer()
        self.tokenizer = TokenizerFactory.create(config.tokenize)
        self.stemmer = StemmerFactory.create(config.stem)

    def run(self, doc):
        text = doc.text
        if self.config.utf8_normalize:
            text = self.normalizer.normalize(text)
        if self.config.lowercase:
            text = text.lower()
        tokens = self.tokenizer.tokenize(text)
        if self.config.stem:
            tokens = self.stemmer.stem(tokens)
        text = ' '.join(tokens)
        return Doc(doc.id, doc.lang, text)
