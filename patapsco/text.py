import contextlib
import io
import itertools
import logging
import pathlib

import nltk
import sacremoses
import spacy
import stanza

from .error import ConfigError
from .pipeline import MultiplexItem
from .util.normalize import NormalizerFactory

LOGGER = logging.getLogger(__name__)


class Stemmer:
    """Stemmer interface"""

    def __init__(self, lang):
        self.lang = lang

    def stem(self, tokens):
        """Stem the tokens

        Args:
            tokens (list of str)

        Returns:
            list: A list of strings
        """
        pass


class PorterStemmer(Stemmer):
    """Porter stemmer from nltk"""

    def __init__(self, lang):
        if lang != "en":
            raise ConfigError("Porter stemmer only supports English")
        super().__init__(lang)
        self.stemmer = nltk.stem.porter.PorterStemmer()

    def stem(self, tokens):
        return [self.stemmer.stem(token, to_lowercase=False) for token in tokens]


class Tokenizer:
    """Tokenizer interface"""

    def __init__(self, lang, model_path):
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


class StanzaNLP(Tokenizer, Stemmer):
    """Tokenizer that uses Stanford's stanza library"""

    def __init__(self, lang, model_path, stem):
        """
        Args:
            lang (str): Language code.
            model_path (str): Path to stanza models.
            stem (bool): Whether to stem the tokens.
        """
        Stemmer.__init__(self, lang)
        Tokenizer.__init__(self, lang, model_path)
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
            elif stem:
                processors = 'tokenize,lemma'
            else:
                processors = 'tokenize'
            self.nlp = stanza.Pipeline(self.lang, processors=processors, dir=self.model_path)
            self.cache = None
        LOGGER.debug(buffer.getvalue())

    def tokenize(self, text):
        doc = self.nlp(text)
        self.cache = doc  # cache the document for the stem method to pick up
        tokens = []
        for sentence in doc.sentences:
            for word in sentence.words:
                tokens.append(word.text)
        return tokens

    ARABIC_DIACRITICS = {'\u064b', '\u064c', '\u064d', '\u064e', '\u064f', '\u0650', '\u0651', '\u0652'}
    diacritic_remove = str.maketrans('', '', ''.join(ARABIC_DIACRITICS))

    def stem(self, tokens):
        tokens = []
        for sentence in self.cache.sentences:
            for token in sentence.words:
                if token.lemma:
                    # TODO Persian lemmas sometimes have # characters in them
                    if self.lang == 'ar':
                        # Arabic lemmas have full diacritization
                        token.lemma = token.lemma.translate(self.diacritic_remove)
                    tokens.append(token.lemma)
                else:
                    tokens.append(token.text)
        return tokens

    @staticmethod
    def _setup_logging():
        stanza_logger = logging.getLogger('stanza')
        patapsco_logger = logging.getLogger('patapsco')
        stanza_logger.setLevel(patapsco_logger.level)
        stanza_logger.handlers = []
        for handler in patapsco_logger.handlers:
            stanza_logger.addHandler(handler)


class SpacyModelLoader:
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

    exclude = ['ner', 'parser']
    disable = ['tok2vec', 'tagger', 'attribute_ruler', 'lemmatizer', 'morphologizer']

    loaders = {}

    @classmethod
    def get_loader(cls, model_path):
        # use this so that models are shared across tasks
        if model_path not in cls.loaders:
            cls.loaders[model_path] = SpacyModelLoader(model_path)
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
            nlp = spacy.load(str(path), exclude=self.exclude, disable=self.disable)
        else:
            # probably not on grid so we try to load locally or download
            model_name = self.model_info[lang]['name']
            if not spacy.util.is_package(model_name):
                # install as a pip package
                LOGGER.info(f"Downloading the {lang} spacy model. This may take a few minutes...")
                spacy.cli.download(model_name)
            LOGGER.info(f"Loading the {lang} spacy model")
            nlp = spacy.load(model_name, exclude=self.exclude, disable=self.disable)
        self.models[lang] = nlp
        return nlp


class SpacyNLP(Tokenizer, Stemmer):
    """Tokenizer that uses the spaCy package"""

    def __init__(self, lang, model_path, stem):
        """
        Args:
            lang (str): Language code.
            model_path (str): Path to stored models.
            stem (bool): Whether to stem the tokens.
        """
        Stemmer.__init__(self, lang)
        Tokenizer.__init__(self, lang, model_path)
        self.nlp = SpacyModelLoader.get_loader(model_path).load(lang)
        self.cache = None
        if stem:
            if lang in ['ar', 'fa', 'zh']:
                raise ConfigError(f"Spacy does not support lemmatization for {lang}")
            # enable pipeline components that the lemmatizer depends on
            names = self.nlp.component_names
            for name in set(names) & {'tok2vec', 'tagger', 'attribute_ruler', 'lemmatizer', 'morphologizer'}:
                self.nlp.enable_pipe(name)

    def tokenize(self, text):
        tokens = self.nlp(text)
        self.cache = tokens
        return [str(token) for token in tokens]

    def stem(self, tokens):
        tokens = self.cache
        return [token.lemma_ if token.lemma_ else token.text for token in tokens]


class MosesTokenizer(Tokenizer):
    """Tokenizer that uses sacremoses"""

    languages = {
        'ar': 'en',
        'en': 'en',
        'fa': 'en',
        'ru': 'ru',
    }

    def __init__(self, lang, model_path):
        """
        Args:
            lang (str): Language code.
            model_path (str): Path to spaCy models.
        """
        super().__init__(lang, model_path)
        if lang == 'zh':
            raise ConfigError("MosesTokenizer does not support Chinese.")
        self.tokenizer = sacremoses.MosesTokenizer(lang=self.languages[lang])
        # we need to segment sentences with spaCy before running the tokenizer
        self.nlp = SpacyModelLoader.get_loader(model_path).load(lang)
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

    def __init__(self, lang, model_path):
        """
        Args:
            lang (str): Language code.
            model_path (str): Path to spaCy models.
        """
        super().__init__(lang, model_path)
        self.n = self.languages[lang]
        # segment sentences with spaCy before create ngrams
        self.nlp = SpacyModelLoader.get_loader(model_path).load(lang)
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

    Tokenizer and stemmer combinations:
     - stanza + stanza (ar, en, fa, ru), porter (en)
     - jieba + no stemming (zh)
     - spacy + spacy (en, ru), porter (en)
     - spacy + no stemming (ar, fa, zh)
     - moses + porter (en)
     - ngrams + no stemming
     - whitespace + porter (en)
    """
    def __init__(self, config, lang):
        """
        Args:
            config (TextProcessorConfig)
            lang (str): Language code
        """
        self.config = config
        self.lang = lang
        self._validate_config(config, lang)
        self.normalizer = NormalizerFactory.create(lang)
        self.tokenizer, self.stemmer = self._create_tokenizer_and_stemmer(config, lang)
        if self.config.stopwords:
            self.stopwords = StopWordsRemoval(self.config.stopwords, lang)
        else:
            self.stopwords = None

    @staticmethod
    def _validate_config(config, lang):
        # TODO need better checks
        if config.tokenize not in ['moses', 'ngram', 'spacy', 'stanza', 'whitespace']:
            raise ConfigError(f"Unknown tokenizer {config.tokenize}")
        if config.stem and config.tokenize == 'ngram':
            raise ConfigError(f"Cannot use stemming with the ngram tokenizer")
        if config.stem and config.stem != 'porter' and config.tokenize in ['moses', 'whitespace']:
            raise ConfigError(f"Cannot use stemming with the tokenizer {config.tokenize}")
        if config.stem and lang == 'zh':
            raise ConfigError(f"Cannot use stemming with language {lang}")

    def _create_tokenizer_and_stemmer(self, config, lang):
        tokenizer_name = config.tokenize
        use_stemmer = bool(config.stem)
        stemmer = tokenizer = None
        model_path = self._get_model_path(tokenizer_name, config.model_path)

        if use_stemmer:
            if tokenizer_name == "spacy":
                tokenizer = stemmer = SpacyNLP(lang, model_path, stem=True)
            elif tokenizer_name == "stanza":
                tokenizer = stemmer = StanzaNLP(lang, model_path, stem=True)
            elif config.stem == "porter":
                tokenizer = WhiteSpaceTokenizer(lang, model_path)
                stemmer = PorterStemmer(lang)
        else:
            if tokenizer_name == 'spacy':
                tokenizer = SpacyNLP(lang, model_path, stem=False)
            elif tokenizer_name == 'stanza':
                tokenizer = StanzaNLP(lang, model_path, stem=False)
            elif tokenizer_name == 'moses':
                tokenizer = MosesTokenizer(lang, model_path)
            elif tokenizer_name == 'ngram':
                tokenizer = NgramTokenizer(lang, model_path)
            elif tokenizer_name == 'whitespace':
                tokenizer = WhiteSpaceTokenizer(lang, model_path)
        return tokenizer, stemmer

    @staticmethod
    def _get_model_path(name, path):
        model_directory_defaults = {
            'ngram': '/exp/scale21/resources/spacy',
            'moses': '/exp/scale21/resources/spacy',
            'spacy': '/exp/scale21/resources/spacy',
            'stanza': '/exp/scale21/resources/stanza',
            'whitespace': None,
        }
        if path:
            return path
        return model_directory_defaults[name]

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
