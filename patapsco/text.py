import contextlib
import io
import itertools
import logging
import pathlib

import sacremoses

from .error import ConfigError
from .pipeline import Task
from .util.file import create_path
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
        if lang != "eng":
            raise ConfigError("Porter stemmer only supports English")
        import nltk  # importing nltk is slow so lazy load
        super().__init__(lang)
        self.stemmer = nltk.stem.porter.PorterStemmer()

    def stem(self, tokens):
        return [self._stem(token) for token in tokens]

    def _stem(self, token):
        try:
            return self.stemmer.stem(token, to_lowercase=False)
        except RecursionError:
            return token


class FarsiStemmer(Stemmer):
    """Porter stemmer from nltk"""

    def __init__(self, lang):
        if lang != "fas":
            raise ConfigError("Parsivar stemmer only supports English")
        import parsivar  # importing nltk is slow so lazy load
        super().__init__(lang)
        self.stemmer = parsivar.FindStems()

    def stem(self, tokens):
        return [self.stemmer.convert_to_stem(token) for token in tokens]


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

    lang_map = {
        'ara': 'ar',
        'eng': 'en',
        'fas': 'fa',
        'rus': 'ru',
        'zho': 'zh-hans',
    }

    def __init__(self, lang, model_path, stem):
        """
        Args:
            lang (str): Language code.
            model_path (Path): Path to stanza models.
            stem (bool): Whether to stem the tokens.
        """
        Stemmer.__init__(self, lang)
        Tokenizer.__init__(self, lang, model_path)
        import stanza  # lazy load stanza when needed
        import torch
        torch.set_num_threads(1)  # TODO make this configurable
        self.lang = self.lang_map[lang]
        self._setup_logging()
        buffer = io.StringIO()
        with contextlib.redirect_stderr(buffer):
            if not (self.model_path / self.lang).exists():
                try:
                    stanza.download(self.lang, model_dir=str(self.model_path))
                except PermissionError:
                    msg = f"Cannot write to {self.model_path}. Maybe model_path needs to be set in process section."
                    raise ConfigError(msg)
            if self.lang == 'zh-hans':
                processors = {'tokenize': 'jieba'}
                package = None
            elif stem:
                processors = 'tokenize,lemma'
                package = 'default'
            else:
                processors = 'tokenize'
                package = 'default'
            self.nlp = stanza.Pipeline(self.lang, processors=processors, package=package, dir=str(self.model_path))
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
        },
        'xx': {  # UD multilang model. TOKEN_ACC: 99.29, SENT_F: 86.39
            'name': 'xx_sent_ud_sm',
            'version': '3.0.0',
        },
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
        """
        Args:
            model_path (Path): Path to spacy model directory
        """
        self.models = {}
        self.model_path = model_path

    def load(self, lang):
        """Load the model (or return cached model)"""
        if lang in self.models:
            return self.models[lang]

        if lang not in self.model_info:
            raise ConfigError(f"Unexpected language for spacy: {lang}")
        import spacy  # lazy load spacy when needed
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

    lang_map = {
        'ara': 'ar',
        'eng': 'en',
        'fas': 'fa',
        'rus': 'ru',
        'zho': 'zh',
    }

    def __init__(self, lang, model_path, stem):
        """
        Args:
            lang (str): Language code.
            model_path (Path): Path to stored models.
            stem (bool): Whether to stem the tokens.
        """
        Stemmer.__init__(self, lang)
        Tokenizer.__init__(self, lang, model_path)
        self.lang = self.lang_map[lang]
        self.nlp = SpacyModelLoader.get_loader(model_path).load(self.lang)
        self.cache = None
        if stem:
            if self.lang in ['ar', 'fa', 'zh']:
                raise ConfigError(f"Spacy does not support lemmatization for {self.lang}")
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
        'ara': 'en',
        'eng': 'en',
        'fas': 'en',
        'rus': 'ru',
    }

    def __init__(self, lang, model_path):
        """
        Args:
            lang (str): Language code.
            model_path (Path): Path to spaCy models.
        """
        super().__init__(lang, model_path)
        if lang == 'zho':
            raise ConfigError("MosesTokenizer does not support Chinese.")
        self.tokenizer = sacremoses.MosesTokenizer(lang=self.languages[lang])
        # we need to segment sentences with spaCy before running the tokenizer
        self.nlp = SpacyModelLoader.get_loader(model_path).load('xx')
        self.nlp.enable_pipe("senter")

    def tokenize(self, text):
        doc = self.nlp(text)
        tokens = itertools.chain.from_iterable(self.tokenizer.tokenize(sent, escape=False) for sent in doc.sents)
        return list(tokens)


class NgramTokenizer(Tokenizer):
    """Character ngram tokenizer"""

    # character ngram size by language
    languages = {
        'ara': 5,
        'eng': 5,
        'fas': 5,
        'rus': 5,
        'zho': 2
    }

    def __init__(self, lang, model_path):
        """
        Args:
            lang (str): Language code.
            model_path (Path): Path to spaCy models.
        """
        super().__init__(lang, model_path)
        self.n = self.languages[lang]
        # segment sentences with spaCy before create ngrams
        self.nlp = SpacyModelLoader.get_loader(model_path).load('xx')
        self.nlp.enable_pipe("senter")

    def tokenize(self, text):
        doc = self.nlp(text)
        ngrams = itertools.chain.from_iterable(self._get_ngrams(sent) for sent in doc.sents)
        return [''.join(x) for x in ngrams]

    def _get_ngrams(self, text):
        # create iterators over characters with an increasing offset and then zip to create ngrams
        text = str(text)
        return zip(*(itertools.islice(chars, offset, None) for offset, chars in enumerate(itertools.tee(text, self.n))))


class StopWordsRemover:
    def __init__(self, source, lang):
        """
        Args:
            source (str): Name of the source of stop words.
            lang (str): Language code.
        """
        filename = lang + ".txt"
        path = pathlib.Path(__file__).parent / 'resources' / 'stopwords' / source / filename
        with open(path, 'r') as fp:
            self.stop_words = {word.strip() for word in fp if word[0] != '#'}

    def identify(self, tokens, is_lower=False):
        """Identify words to remove

        Args:
            tokens (list of str)
            is_lower (bool) Whether the tokens have already been lowercased.

        Returns
            indices of tokens to remove
        """
        if is_lower:
            tokens = [index for index, token in enumerate(tokens) if token in self.stop_words]
        else:
            tokens = [index for index, token in enumerate(tokens) if token.lower() in self.stop_words]
        return tokens

    def remove(self, tokens, indices):
        """Remove stop words

        Args:
            tokens (list of str)
            indices (list of int)

        Returns
            list of str
        """
        return [token for index, token in enumerate(tokens) if index not in indices]


class TokenizerStemmerFactory:
    """Constructs tokenizers and stemmers based on configurations."""

    tokenizers = {'jieba', 'moses', 'ngram', 'spacy', 'stanza', 'whitespace'}
    stemmers = {'porter', 'spacy', 'stanza', 'parsivar'}
    # key is name:lang
    tokenizer_cache = {}
    stemmer_cache = {}

    @classmethod
    def validate(cls, config, lang):
        """Validate the config for tokenizers and stemmers.

        Allowed tokenizer and stemmer combinations:
         - stanza + stanza (ara, eng, fas, rus) or porter (eng)
         - jieba/stanza + no stemming (zho)
         - spacy + spacy (eng, rus) or porter (eng)
         - spacy + no stemming (ara, fas, zho)
         - moses + porter (eng)
         - ngram + no stemming
         - whitespace + porter (eng)
         - not ngram + parsivar (fas)

        Some language restrictions are left to the tokenizers or stemmers to check:
         - No Chinese stemming.
         - Spacy has no stemming for Arabic or Farsi.
         - Porter stemming only works for English.
         - Parsivar stemming only works for Farsi.
        """
        if config.tokenize not in cls.tokenizers:
            raise ConfigError(f"Unknown tokenizer {config.tokenize}")
        if config.stem and config.stem not in cls.stemmers:
            raise ConfigError(f"Unknown stemmer {config.stem}")
        if config.stem:
            if config.tokenize in ['jieba', 'ngram']:
                raise ConfigError(f"Cannot tokenize with {config.tokenize} and also stem.")
            if config.tokenize in ['moses', 'whitespace'] and config.stem != 'porter' and config.stem != 'parsivar':
                raise ConfigError(f"Incompatible tokenizer ({config.tokenize}) and stemmer ({config.stem})")
            if lang == "eng":
                if config.tokenize == "spacy" and config.stem not in ['porter', 'spacy']:
                    raise ConfigError(f"Incompatible tokenizer ({config.tokenize}) and stemmer ({config.stem})")
                if config.tokenize == "stanza" and config.stem not in ['porter', 'stanza']:
                    raise ConfigError(f"Incompatible tokenizer ({config.tokenize}) and stemmer ({config.stem})")
            else:
                if config.tokenize in ["spacy", "stanza"] and config.tokenize != config.stem and config.stem != 'parsivar':
                    raise ConfigError(f"Incompatible tokenizer ({config.tokenize}) and stemmer ({config.stem})")

    @classmethod
    def create_tokenizer(cls, config, lang):
        key = f"{config.tokenize}:{lang}"
        if key in cls.tokenizer_cache:
            return cls.tokenizer_cache[key]

        tokenizer_name = config.tokenize
        # jieba is wrapped by stanza
        tokenizer_name = tokenizer_name if tokenizer_name != 'jieba' else 'stanza'
        use_stemmer = bool(config.stem)
        tokenizer = None
        model_path = cls._get_model_path(tokenizer_name, config.model_path)

        if tokenizer_name in ['spacy', 'stanza']:
            # if not porter stemming, the tokenizer also implements stemming
            also_stemmer = use_stemmer and config.stem != 'porter' and config.stem != 'parsivar'
            if tokenizer_name == 'spacy':
                tokenizer = SpacyNLP(lang, model_path, stem=also_stemmer)
            elif tokenizer_name == 'stanza':
                tokenizer = StanzaNLP(lang, model_path, stem=also_stemmer)
        elif tokenizer_name == 'moses':
            tokenizer = MosesTokenizer(lang, model_path)
        elif tokenizer_name == 'ngram':
            tokenizer = NgramTokenizer(lang, model_path)
        elif tokenizer_name == 'whitespace':
            tokenizer = WhiteSpaceTokenizer(lang, model_path)

        cls.tokenizer_cache[key] = tokenizer
        return tokenizer

    @classmethod
    def create_stemmer(cls, config, lang):
        if not config.stem:
            return None
        key = f"{config.stem}:{lang}"
        if key in cls.stemmer_cache:
            return cls.stemmer_cache[key]

        stemmer_name = config.stem
        stemmer = None

        if stemmer_name in ['spacy', 'stanza']:
            return cls.create_tokenizer(config, lang)
        elif stemmer_name == 'porter':
            stemmer = PorterStemmer(lang)
        elif stemmer_name == 'parsivar':
            stemmer = FarsiStemmer(lang)

        cls.stemmer_cache[key] = stemmer
        return stemmer

    @staticmethod
    def _get_model_path(name, path):
        """returns a Path object"""
        model_directory_defaults = {
            'ngram': '/exp/scale21/resources/spacy',
            'moses': '/exp/scale21/resources/spacy',
            'spacy': '/exp/scale21/resources/spacy',
            'stanza': '/exp/scale21/resources/stanza',
            'whitespace': None,
        }
        if path:
            return create_path(path)
        else:
            path = model_directory_defaults[name]
            return create_path(path) if path else path


class TextProcessor(Task):
    """Normalizes, segments, and performs other standardization on text

    Used on both documents and queries.
    """
    def __init__(self, run_path, config, lang):
        """
        Args:
            run_path (str): Root directory of the run.
            config (TextProcessorConfig)
            lang (str): Language code
        """
        super().__init__(run_path)
        self.lang = lang
        self.processor_config = config
        TokenizerStemmerFactory.validate(config, lang)
        self.normalizer = None
        self.tokenizer = None
        self.stemmer = None
        self.stopword_remover = None

    def begin(self):
        self.normalizer = NormalizerFactory.create(self.lang, self.processor_config.normalize)
        self.tokenizer = TokenizerStemmerFactory.create_tokenizer(self.processor_config, self.lang)
        self.stemmer = TokenizerStemmerFactory.create_stemmer(self.processor_config, self.lang)
        if self.processor_config.stopwords:
            self.stopword_remover = StopWordsRemover(self.processor_config.stopwords, self.lang)

    def process(self, item):
        """Child classes will override this"""
        return item

    def pre_normalize(self, text):
        return self.normalizer.pre_normalize(text)

    def post_normalize(self, text):
        return self.normalizer.post_normalize(text)

    def tokenize(self, text):
        return self.tokenizer.tokenize(text)

    def identify_stop_words(self, tokens, is_lower=False):
        if self.stopword_remover:
            return self.stopword_remover.identify(tokens, is_lower)
        else:
            return []

    def remove_stop_words(self, tokens, indices):
        if self.stopword_remover:
            return self.stopword_remover.remove(tokens, indices)
        else:
            return tokens

    def stem(self, tokens):
        if self.stemmer:
            return self.stemmer.stem(tokens)
        else:
            return tokens
