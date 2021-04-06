from .config import BaseConfig
from .util import ComponentFactory


class TokenizeConfig(BaseConfig):
    name: str


class TokenizerFactory(ComponentFactory):
    classes = {
        'whitespace': 'WhiteSpaceTokenizer',
    }
    config_class = TokenizeConfig


class Tokenizer:
    """Tokenizer interface"""

    def __init__(self, config):
        self.config = config

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


class StemConfig(BaseConfig):
    name: str


class TruncStemConfig(BaseConfig):
    name: str
    length: int


class StemmerFactory(ComponentFactory):
    classes = {
        'trunc': 'TruncatingStemmer',
    }
    config_class = TokenizeConfig


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


class Normalizer:
    def normalize(self, text):
        return text


class TextProcessor:
    def __init__(self, config):
        self.config = config
        self.normalizer = Normalizer()
        self.tokenizer = TokenizerFactory.create(config.tokenize)
        self.stemmer = StemmerFactory.create(config.stem)

    def normalize(self, text):
        return self.normalizer.normalize(text)

    def lowercase_text(self, text):
        return text.lower()

    def lowercase_tokens(self, tokens):
        return [token.lower() for token in tokens]

    def tokenize(self, text):
        return self.tokenizer.tokenize(text)

    def stem(self, tokens):
        return self.stemmer.stem(tokens)
