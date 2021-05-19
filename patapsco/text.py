import contextlib
import io
import itertools
import logging
import pathlib

import sacremoses
import scriptnorm
import spacy
import stanza


from .error import ConfigError
from .pipeline import MultiplexItem
from .schema import TokenizeConfig, StemConfig
from .util import ComponentFactory

LOGGER = logging.getLogger(__name__)


class TokenizerFactory(ComponentFactory):
    classes = {
        'moses': 'MosesTokenizer',
        'spacy': 'SpaCyTokenizer',
        'stanza': 'StanzaTokenizer',
        'whitespace': 'WhiteSpaceTokenizer',
    }
    model_directory_defaults = {
        'moses': '/exp/scale21/resources/spacy',
        'spacy': '/exp/scale21/resources/spacy',
        'stanza': '/exp/scale21/resources/stanza',
        'whitespace': None,
    }
    config_class = TokenizeConfig

    @classmethod
    def create(cls, config, *args, **kwargs):
        """
        Args:
            config (TokenizeConfig)
        """
        if not config.path:
            try:
                config.path = cls.model_directory_defaults[config.name]
            except KeyError:
                raise ConfigError(f"Unknown tokenizer: {config.name}")
        kwargs['model_path'] = config.path
        return super().create(config, *args, **kwargs)


class StemmerFactory(ComponentFactory):
    classes = {
        'mock': 'MockStemmer',
    }
    config_class = StemConfig


class Normalizer:
    def __init__(self, lang):
        self.lang = lang

    def normalize(self, text):
        return scriptnorm.process(self.lang, text)


class Tokenizer:
    """Tokenizer interface"""

    def __init__(self, config, lang, model_path):
        self.config = config
        self.lang = lang.lower()
        self.model_path = model_path

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


class StanzaTokenizer(Tokenizer):
    """Tokenizer that uses Stanford's stanza library"""

    def __init__(self, config, lang, model_path):
        super().__init__(config, lang, model_path)
        if self.lang == 'zh':
            self.lang = 'zh-hans'
        self._setup_logging()
        buffer = io.StringIO()
        with contextlib.redirect_stderr(buffer):
            try:
                stanza.download(self.lang, model_dir=self.model_path)
            except PermissionError:
                raise ConfigError(f"Cannot write to {self.model_path}. Maybe tokenize.path needs to be set.")
            if self.lang == 'zh-hans':
                processors = {'tokenize': 'jieba'}
            else:
                processors = 'tokenize'
            self.nlp = stanza.Pipeline(self.lang, processors=processors, dir=self.model_path)
        LOGGER.debug(buffer.getvalue())

    def tokenize(self, text):
        doc = self.nlp(text)
        tokens = []
        for sentence in doc.sentences:
            for word in sentence.words:
                tokens.append(word.text)
        return tokens

    @staticmethod
    def _setup_logging():
        stanza_logger = logging.getLogger('stanza')
        patapsco_logger = logging.getLogger('patapsco')
        stanza_logger.setLevel(patapsco_logger.level)
        stanza_logger.handlers = []
        for handler in patapsco_logger.handlers:
            stanza_logger.addHandler(handler)


class SpaCyModelLoader:
    """Load the spaCy model and install if needed"""

    model_info = {
        'ar': {  # UD multilang model. TOKEN_ACC: 99.29, SENT_F: 86.39
            'name': 'xx_sent_ud_sm',
            'version': '3.0.0'
        },
        'en': {  # TOKEN_ACC: 99.93, SENT_F: 89.02
            'name': 'en_core_web_md',
            'version': '3.0.0',
        },
        'fa': {  # UD multilang model. TOKEN_ACC: 99.29, SENT_F: 86.39
            'name': 'xx_sent_ud_sm',
            'version': '3.0.0',
        },
        'ru': {  # TOKEN_ACC: 99.85, SENT_F: 99.85
            'name': 'ru_core_news_sm',
            'version': '3.0.0',
        },
        'zh': {  # TOKEN_ACC: 97.88, SENT_F: 75.88
            'name': 'zh_core_web_md',
            'version': '3.0.0',
        }
    }

    exclude = ['tok2vec', 'morphologizer', 'tagger', 'parser', 'ner', 'attribute_ruler', 'lemmatizer']

    loaders = {}

    @classmethod
    def get_loader(cls, model_path):
        # use this so that models are shared across tasks
        if model_path not in cls.loaders:
            cls.loaders[model_path] = SpaCyModelLoader(model_path)
        return cls.loaders[model_path]

    def __init__(self, model_path):
        self.models = {}
        self.model_path = pathlib.Path(model_path)

    def load(self, lang):
        """Load the model (or return cached model)"""
        if lang in self.models:
            return self.models[lang]

        if lang not in self.model_info:
            raise ConfigError(f"Unexpected language for spacy: {lang}")
        path = self.model_path / f"{self.model_info[lang]['name']}-{self.model_info[lang]['version']}"
        if path.exists():
            LOGGER.info(f"Loading the {lang} spacy model")
            nlp = spacy.load(str(path), exclude=self.exclude)
        else:
            # probably not on grid so we try to load locally or download
            model_name = self.model_info[lang]['name']
            if not spacy.util.is_package(model_name):
                # install as a pip package
                LOGGER.info(f"Downloading the {lang} spacy model. This may take a few minutes...")
                spacy.cli.download(model_name)
            LOGGER.info(f"Loading the {lang} spacy model")
            nlp = spacy.load(model_name, exclude=self.exclude)
        self.models[lang] = nlp
        return nlp


class SpaCyTokenizer(Tokenizer):
    """Tokenizer that uses the spaCy package"""

    def __init__(self, config, lang, model_path):
        super().__init__(config, lang, model_path)
        self.nlp = SpaCyModelLoader.get_loader(model_path).load(lang)

    def tokenize(self, text):
        tokens = self.nlp(text)
        return [str(token) for token in tokens]


class MosesTokenizer(Tokenizer):
    """Tokenizer that uses sacremoses"""

    languages = {
        'ar': 'en',
        'en': 'en',
        'fa': 'en',
        'ru': 'ru',
    }

    def __init__(self, config, lang, model_path):
        super().__init__(config, lang, model_path)
        if lang == 'zh':
            raise ConfigError("MosesTokenizer does not support Chinese.")
        self.tokenizer = sacremoses.MosesTokenizer(lang=self.languages[lang])
        # we need to segment sentences with spaCy before running the tokenizer
        self.nlp = SpaCyModelLoader.get_loader(model_path).load(lang)
        self.nlp.enable_pipe("senter")

    def tokenize(self, text):
        doc = self.nlp(text)
        tokens = itertools.chain.from_iterable(self.tokenizer.tokenize(sent, escape=False) for sent in doc.sents)
        return list(tokens)


class NgramTokenizer(Tokenizer):
    """Character ngram tokenizer"""

    languages = {
        'ar': 5,
        'en': 5,
        'fa': 5,
        'ru': 5,
        'zh': 2
    }

    def __init__(self, config, lang, model_path):
        super().__init__(config, lang, model_path)
        self.n = self.languages[lang]
        # segment sentences with spaCy before create ngrams
        self.nlp = SpaCyModelLoader.get_loader(model_path).load(lang)
        self.nlp.enable_pipe("senter")

    def tokenize(self, text):
        doc = self.nlp(text)
        ngrams = itertools.chain.from_iterable(self._get_ngrams(sent) for sent in doc.sents)
        return [''.join(x) for x in ngrams]

    def _get_ngrams(self, text):
        # create iterators over characters with an increasing offset and then zip to create ngrams
        text = str(text)
        return zip(*(itertools.islice(chars, offset, None) for offset, chars in enumerate(itertools.tee(text, self.n))))


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


class MockStemmer(Stemmer):
    def __init__(self, config, lang):
        super().__init__(config, lang)
        self.length = 5

    def stem(self, tokens):
        return [x[:self.length] for x in tokens]


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
