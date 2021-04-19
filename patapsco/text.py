import pathlib

from .config import BaseConfig
from .util import ComponentFactory


class TokenizeConfig(BaseConfig):
    name: str


class TokenizerFactory(ComponentFactory):
    classes = {
        'whitespace': 'WhiteSpaceTokenizer',
    }
    config_class = TokenizeConfig


class StemConfig(BaseConfig):
    name: str


class TruncStemConfig(BaseConfig):
    name: str
    length: int


class StemmerFactory(ComponentFactory):
    classes = {
        'trunc': 'TruncatingStemmer',
    }
    config_class = TruncStemConfig


class Normalizer:
    def __init__(self, lang):
        pass

    def normalize(self, text):
        return text


class Tokenizer:
    """Tokenizer interface"""

    def __init__(self, config, lang):
        self.config = config
        self.lang = lang

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


class StopWordsRemoval:
    def __init__(self, source, lang):
        filename = lang + ".txt"
        path = pathlib.Path(__file__).parent / 'resources' / 'stopwords' / source / filename
        with open(path, 'r') as fp:
            self.words = {word.strip() for word in fp if word[0] != '#'}

    def remove(self, tokens, lower=False):
        """Remove stop words

        Args:
            tokens (list of str)
            lower (bool) Whether the tokens have already been lowercased.

        Returns
            list of str
        """
        if lower:
            tokens = [token for token in tokens if token.lower() not in self.words]
        else:
            tokens = [token for token in tokens if token not in self.words]
        return tokens


class Stemmer:
    """Stemmer interface"""

    def __init__(self, config, lang):
        self.config = config
        self.lang = lang

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


class TextProcessor:
    def __init__(self, config):
        self.config = config
        self.initialized = False

    def initialize(self, lang):
        self.initialized = True
        self.normalizer = Normalizer(lang)
        self.tokenizer = TokenizerFactory.create(self.config.tokenize, lang)
        self.stemmer = StemmerFactory.create(self.config.stem, lang)
        self.stopwords = StopWordsRemoval(self.config.stopwords, lang)

    def normalize(self, text):
        return self.normalizer.normalize(text)

    def tokenize(self, text):
        return self.tokenizer.tokenize(text)

    def lowercase(self, tokens):
        return [token.lower() for token in tokens]

    def remove_stop_words(self, tokens, lower=False):
        return self.stopwords.remove(tokens, lower)

    def stem(self, tokens):
        return self.stemmer.stem(tokens)
