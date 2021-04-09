import collections
import csv
import json
import pathlib

from .config import BaseConfig, Union
from .pipeline import Task
from .text import TextProcessor, StemConfig, TokenizeConfig, TruncStemConfig
from .util import trec, ComponentFactory
from .util.file import GlobFileGenerator

Topic = collections.namedtuple('Topic', ('id', 'lang', 'title', 'desc', 'narr'))
Query = collections.namedtuple('Query', ('id', 'lang', 'text'))


class InputConfig(BaseConfig):
    """Configuration for Topic input"""
    name: str
    lang: str
    encoding: str = "utf8"
    strip_non_digits: bool = False
    path: Union[str, list]


class ProcessorConfig(BaseConfig):
    """Configuration of the topic processor"""
    name: str = "default"
    query: str = "title"  # field1+field2 where field is title, desc, narr
    utf8_normalize: bool = True
    lowercase: bool = True
    tokenize: TokenizeConfig
    stem: Union[StemConfig, TruncStemConfig]


class TopicReaderFactory(ComponentFactory):
    classes = {
        'trec': 'TrecTopicReader'
    }
    config_class = InputConfig


class TopicProcessorFactory(ComponentFactory):
    classes = {
        'default': 'TopicProcessor'
    }
    config_class = ProcessorConfig


class TrecTopicReader:
    """Iterator over topics from trec sgml"""

    def __init__(self, config):
        self.lang = config.lang
        self.strip_non_digits = config.strip_non_digits
        self.topics = GlobFileGenerator(config.path, trec.parse_topics, 'EN-', config.encoding)

    def __iter__(self):
        return self

    def __next__(self):
        topic = next(self.topics)
        identifier = ''.join(filter(str.isdigit, topic[0])) if self.strip_non_digits else topic[0]
        return Topic(identifier, self.lang, topic[1], topic[2], topic[3])


class TsvTopicReader:
    """Iterator over topics from tsv file """

    def __init__(self, config):
        self.lang = config.lang
        self.topics = GlobFileGenerator(config.path, self.parse, config.encoding)

    def __iter__(self):
        return self

    def __next__(self):
        topic = next(self.topics)
        return Topic(topic[0], self.lang, topic[1], topic[2], topic[3])

    @staticmethod
    def parse(path, encoding='utf8'):
        with open(path, 'r', encoding=encoding) as fp:
            reader = csv.reader(fp, delimiter='\t')
            for line in reader:
                yield line[0], line[1].strip(), None, None


class QueryWriter(Task):
    """Write queries to a file"""

    def __init__(self, path):
        """
        Args:
            path (str): Path of query file to write.
        """
        super().__init__()
        dir = pathlib.Path(path)
        dir.mkdir(parents=True)
        path = dir / 'queries.json'
        self.file = open(path, 'w')

    def process(self, query):
        """
        Args:
            query (Query)

        Returns
            Query
        """

        self.file.write(json.dumps(query._asdict()) + "\n")
        return query

    def end(self):
        self.file.close()


class TopicProcessor(Task, TextProcessor):
    """Topic Preprocessing"""

    def __init__(self, config):
        """
        Args:
            config (ProcessorConfig)
        """
        Task.__init__(self)
        TextProcessor.__init__(self, config)
        self.fields = config.query.split('+')

    def process(self, topic):
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
        return ' '.join([getattr(topic, f).strip() for f in self.fields])
