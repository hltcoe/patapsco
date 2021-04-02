import collections
import json
import pathlib

from .config import BaseConfig, Union
from .error import ConfigError
from .text import TextProcessor, StemConfig, TokenizeConfig, TruncStemConfig
from .util import trec

Topic = collections.namedtuple('Topic', ('id', 'lang', 'title', 'desc', 'narr'))
Query = collections.namedtuple('Query', ('id', 'lang', 'text'))


class InputTopicsConfig(BaseConfig):
    lang: str
    encoding: str = "utf8"
    format: str
    path: str


class TopicReaderFactory:
    @classmethod
    def create(cls, config):
        config = InputTopicsConfig(**config)
        if config.format == "trec":
            return TrecTopicReader(config.path, config.lang, config.encoding)
        else:
            raise ConfigError(f"Unknown topic format: {config.format}")


class TrecTopicReader:
    def __init__(self, path, lang, encoding='utf8'):
        self.lang = lang
        self.topics = trec.parse_topics(path, 'EN-', encoding)

    def __iter__(self):
        return self

    def __next__(self):
        topic = next(self.topics)
        return Topic(topic[0], self.lang, topic[1], topic[2], topic[3])


class QueryWriter:
    def __init__(self, path):
        dir = pathlib.Path(path)
        dir.mkdir(parents=True)
        path = dir / 'queries.json'
        self.file = open(path, 'w')

    def write(self, query):
        self.file.write(json.dumps(query._asdict()) + "\n")

    def close(self):
        self.file.close()


class QueryProcessorConfig(BaseConfig):
    name: str = "default"
    utf8_normalize: bool = True
    lowercase: bool = True
    output: str
    overwrite: bool = False
    tokenize: TokenizeConfig
    stem: Union[StemConfig, TruncStemConfig]


class QueryProcessorFactory:
    classes = {
        'default': 'QueryProcessor'
    }

    @classmethod
    def create(cls, config):
        """
        Args:
            config (dict)
        """
        config = QueryProcessorConfig(**config)
        try:
            class_name = cls.classes[config.name]
        except KeyError:
            raise ConfigError(f"Unknown query processor: {config.name}")
        try:
            class_ = globals()[class_name]
        except KeyError:
            raise RuntimeError(f"Cannot find {class_name} in {cls.__name__}")
        return class_(config)


class QueryProcessor(TextProcessor):
    """Query Preprocessing"""
    def __init__(self, config):
        """
        Args:
            config (QueryProcessorConfig)
        """
        super().__init__(config)

    def run(self, topic):
        """
        Args:
            topic (Topic)

        Returns
            Query
        """
        text = self._select_text(topic)
        if self.config.utf8_normalize:
            text = self.normalize(text)
        if self.config.lowercase:
            text = self.lowercase_text(text)
        tokens = self.tokenize(text)
        if self.config.stem:
            tokens = self.stem(tokens)
        text = ' '.join(tokens)
        return Query(topic.id, topic.lang, text)

    def _select_text(self, topic):
        return topic.title
