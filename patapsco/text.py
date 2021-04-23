import pathlib

from .config import BaseConfig, Optional, Union
from .error import ConfigError
from .pipeline import MultiplexItem
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


class TextProcessorConfig(BaseConfig):
    """Configuration for the text processing"""
    normalize: bool = True
    tokenize: TokenizeConfig
    lowercase: bool = True
    stopwords: Union[None, bool, str] = "lucene"
    stem: Union[None, bool, StemConfig, TruncStemConfig]
    splits: Optional[list]


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


class Splitter:
    """Incrementally accepts output from a text processor task.

    Supports splitting output for multiplexing the pipeline.
    Each output item has an associated name.
    """

    allowed_splits = {"tokenize", "lowercase", "stopwords", "stem"}

    def __init__(self, splits):
        """
        Args:
            splits (list): List of split strings like "tokenize+lowercase"
        """
        if splits:
            self.splits = {split.split('+')[-1]: split for split in splits}
            for name in self.splits.keys():
                if name not in self.allowed_splits:
                    raise ConfigError(f"Unrecognized split: {name}")
        else:
            self.splits = {}
        self.items = MultiplexItem()

    def add(self, key, item):
        if key in self.splits:
            self.items.add(self.splits[key], item)

    def get(self):
        return self.items

    def reset(self):
        self.items = MultiplexItem()

    def __bool__(self):
        return len(self.splits) > 0


class TextProcessor:
    """Normalizes, segments, and performs other standardization on text

    Used on both documents and queries.
    """
    def __init__(self, config, lang):
        """
        Args:
            config (TextProcessorConfig)
            lang (str)
        """
        self.config = config
        self.lang = lang
        self.normalizer = Normalizer(lang)
        self.tokenizer = TokenizerFactory.create(self.config.tokenize, lang)
        if self.config.stem:
            self.stemmer = StemmerFactory.create(self.config.stem, lang)
        else:
            self.stemmer = None
        if self.config.stopwords:
            self.stopwords = StopWordsRemoval(self.config.stopwords, lang)
        else:
            self.stopwords = None

    def normalize(self, text):
        return self.normalizer.normalize(text)

    def tokenize(self, text):
        return self.tokenizer.tokenize(text)

    def lowercase(self, tokens):
        return [token.lower() for token in tokens]

    def remove_stop_words(self, tokens, lower=False):
        if self.stopwords:
            return self.stopwords.remove(tokens, lower)
        else:
            return tokens

    def stem(self, tokens):
        if self.stemmer:
            return self.stemmer.stem(tokens)
        else:
            return tokens
