import contextlib
import io
import itertools
import logging
import pathlib

from .error import ConfigError
from .pipeline import Task
from .util import LangStandardizer
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
    """Farsi stemmer using parsivar"""

    def __init__(self, lang):
        if lang != "fas":
            raise ConfigError("Parsivar stemmer only supports Farsi")
        import parsivar
        super().__init__(lang)
        self.stemmer = parsivar.FindStems()

    def stem(self, tokens):
        return [self.stemmer.convert_to_stem(token) for token in tokens]


class Tokenizer:
    """Tokenizer interface"""

    def __init__(self, lang, model_path):
        """
        Args:
            lang (str): ISO 639-3 language code
            model_path (str|None): Path to model directory or None if default
        """
        self.lang = lang.lower()
        self.model_path = create_path(model_path) if model_path else model_path

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


class MosesTokenizer(Tokenizer):
    """Tokenizer that uses sacremoses

    Relies on the spacy xx sentence segmenter.
    """
    not_supported = {"zho"}  # there are probably many other languages that moses doesn't do well on

    def __init__(self, lang, model_path):
        super().__init__(lang, model_path)
        if self.lang in self.not_supported:
            raise ConfigError(f"Moses tokenizer does not support {self.lang}")
        import sacremoses
        self.tokenizer = sacremoses.MosesTokenizer(lang=LangStandardizer.iso_639_1(self.lang))
        # we need to segment sentences with spaCy before running the tokenizer
        self.nlp = SpacyModelLoader.get_loader(model_path).load('xx')
        self.nlp.enable_pipe("senter")

    def tokenize(self, text):
        doc = self.nlp(text)
        tokens = itertools.chain.from_iterable(self.tokenizer.tokenize(sent, escape=False) for sent in doc.sents)
        return list(tokens)


class JiebaTokenizer(Tokenizer):
    """Tokenizer that uses jieba for Chinese"""

    def __init__(self, lang, model_path):
        super().__init__(lang, model_path)
        if self.lang != 'zho':
            raise ConfigError("Jieba tokenizer only supports zho")
        import jieba
        jieba.setLogLevel(60)
        self.tokenizer = jieba

    def tokenize(self, text):
        return list(self.tokenizer.cut(text, cut_all=False))


class NgramTokenizer(Tokenizer):
    """Character ngram tokenizer

    TODO: make the number of characters configurable
    """

    # character ngram size by language
    cjk_codes = {'zho', 'jpn', 'kor'}

    def __init__(self, lang, model_path):
        super().__init__(lang, model_path)
        self.n = 2 if self.lang in self.cjk_codes else 5
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
            lang (str): ISO 639-3 language code.
            model_path (str): Path to stanza model directory.
            stem (bool): Whether to stem the tokens.
        """
        Stemmer.__init__(self, lang)
        Tokenizer.__init__(self, lang, model_path)
        import stanza  # lazy load stanza when needed
        import torch
        torch.set_num_threads(1)  # TODO make this configurable
        self.lang = self.lang_map[self.lang]
        self._setup_logging()
        buffer = io.StringIO()
        with contextlib.redirect_stderr(buffer):
            if self.model_path and not (self.model_path / self.lang).exists():
                try:
                    stanza.download(self.lang, model_dir=str(self.model_path))
                except PermissionError:
                    msg = f"Cannot write to {self.model_path}. Maybe model_path needs to be set in process section."
                    raise ConfigError(msg)
            if self.lang == 'zh-hans':
                processors = 'tokenize'
                package = 'default'
            elif stem:
                processors = 'tokenize,lemma'
                package = 'default'
            else:
                processors = 'tokenize'
                package = 'default'
            # self.nlp = stanza.Pipeline(self.lang, processors=processors, package=package, dir=str(self.model_path))
            self.nlp = stanza.Pipeline(self.lang, processors=processors, package=package)
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
    """Load the spaCy model and install if not already available.

    By default, spacy will download the models into the site packages of the virtual environment.
    Setting the model path changes this to load from that directory.
    """

    # models are listed at https://spacy.io/usage/models
    # this list is accurate as of spacy 3.1.0
    model_map = {
        'ca': 'ca_core_news_sm',
        'da': 'da_core_news_sm',
        'de': 'de_core_news_sm',
        'el': 'el_core_news_sm',
        'en': 'en_core_web_sm',
        'es': 'es_core_news_sm',
        'fr': 'fr_core_news_sm',
        'it': 'it_core_news_sm',
        'ja': 'ja_core_news_sm',
        'lt': 'lt_core_news_sm',
        'mk': 'mk_core_news_sm',
        'nb': 'nb_core_news_sm',
        'nl': 'nl_core_news_sm',
        'pl': 'pl_core_news_sm',
        'pt': 'pt_core_news_sm',
        'ro': 'ro_core_news_sm',
        'ru': 'ru_core_news_sm',
        'zh': 'zh_core_web_sm',
        'xx': 'xx_sent_ud_sm',  # multi-language sentence segmentation
    }

    exclude = ['ner', 'parser']
    disable = ['tok2vec', 'tagger', 'attribute_ruler', 'lemmatizer', 'morphologizer']

    loaders = {}

    @classmethod
    def get_loader(cls, model_path):
        """
        Get a model loader for a particular model directory.
        Use this so that models are shared across tasks.

        Args:
            model_path (Path): Path to spacy model directory or None for default.

        Returns:
            SpacyModelLoader
        """
        if model_path not in cls.loaders:
            cls.loaders[model_path] = SpacyModelLoader(model_path)
        return cls.loaders[model_path]

    def __init__(self, model_path):
        """
        Args:
            model_path (Path|None): Path to spacy model directory or None for default.
        """
        self.models = {}
        self.model_path = model_path

    def load(self, lang):
        """Load the model (or return cached model)"""
        if lang in self.models:
            return self.models[lang]

        if lang == 'xx':
            # multi-language model
            iso_639_1 = 'xx'
        else:
            iso_639_1 = LangStandardizer.iso_639_1(lang)
        if iso_639_1 not in self.model_map:
            iso_639_1 = 'xx'  # fallback to multi-language model
        import spacy  # lazy load spacy when needed
        if self.model_path:
            raise NotImplementedError("Spacy model loading from a directory is not available yet")
        else:
            model_name = self.model_map[iso_639_1]
            if not spacy.util.is_package(model_name):
                # install as a pip package
                LOGGER.info(f"Downloading the {lang} spacy model. This may take a few minutes...")
                spacy.cli.download(model_name)
            LOGGER.info(f"Loading the {lang} spacy model")
            nlp = spacy.load(model_name, exclude=self.exclude, disable=self.disable)
        self.models[lang] = nlp
        return nlp


class SpacyNLP(Tokenizer, Stemmer):
    """Tokenizer and optional stemmer that uses the spaCy package"""

    def __init__(self, lang, model_path, stem):
        """
        Args:
            lang (str): Language code.
            model_path (str|None): Path to stored models.
            stem (bool): Whether to stem the tokens.
        """
        Stemmer.__init__(self, lang)
        Tokenizer.__init__(self, lang, model_path)
        self.nlp = SpacyModelLoader.get_loader(self.model_path).load(self.lang)
        self.cache = None
        if stem:
            # if self.lang in ['ar', 'fa', 'zh']:
            #     raise ConfigError(f"Spacy does not support lemmatization for {self.lang}")
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
    """Constructs tokenizers and stemmers (lemmatizers) based on configurations.

    Stanza and spacy are full NLP pipelines so the tokenizer and stemmer are loaded together.
    The factory also does
    """

    tokenizers = {'jieba', 'moses', 'ngram', 'spacy', 'stanza', 'whitespace'}
    stemmers = {'porter', 'spacy', 'stanza', 'parsivar'}
    # key is name:lang
    tokenizer_cache = {}
    stemmer_cache = {}

    @classmethod
    def validate(cls, config, lang):
        """Validate the config for tokenizers and stemmers.

        Args:
            config (TextProcessorConfig): text configuration
            lang (str): ISO 639-3 language code
        """
        if config.tokenize not in cls.tokenizers:
            raise ConfigError(f"Unknown tokenizer {config.tokenize}")
        if config.stem and config.stem not in cls.stemmers:
            raise ConfigError(f"Unknown stemmer {config.stem}")
        if config.stem:
            if config.tokenize == 'ngram':
                raise ConfigError("ngram tokenizer not compatible with stemming")
            if config.stem == 'spacy' and config.tokenize != 'spacy':
                raise ConfigError("If using spacy stemming, must use spacy tokenizer")
            if config.stem == 'stanza' and config.tokenize != 'stanza':
                raise ConfigError("If using stanza stemming, must use stanza tokenizer")

    @classmethod
    def create_tokenizer(cls, config, lang):
        """
        Args:
            config (TextProcessConfig): text configuration
            lang (str): ISO 639-3 language code

        Returns:
            Tokenizer
        """
        key = f"{config.tokenize}:{lang}"
        if key in cls.tokenizer_cache:
            return cls.tokenizer_cache[key]

        if config.tokenize in ['spacy', 'stanza']:
            also_stemmer = config.stem == config.tokenize
            if config.tokenize == 'spacy':
                tokenizer = SpacyNLP(lang, config.model_path, stem=also_stemmer)
            else:
                tokenizer = StanzaNLP(lang, config.model_path, stem=also_stemmer)
        elif config.tokenize == 'jieba':
            tokenizer = JiebaTokenizer(lang, config.model_path)
        elif config.tokenize == 'moses':
            tokenizer = MosesTokenizer(lang, config.model_path)
        elif config.tokenize == 'ngram':
            tokenizer = NgramTokenizer(lang, config.model_path)
        elif config.tokenize == 'whitespace':
            tokenizer = WhiteSpaceTokenizer(lang, config.model_path)
        else:
            raise ConfigError(f"Unknown tokenizer {config.tokenize}")

        cls.tokenizer_cache[key] = tokenizer
        return tokenizer

    @classmethod
    def create_stemmer(cls, config, lang):
        """
        Args:
            config (TextProcessConfig): text configuration
            lang (str): ISO 639-3 language code

        Returns:
            Stemmer
        """
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
