import csv
import dataclasses
import json
import pathlib
from typing import Optional

from .error import ConfigError, ParseError
from .pipeline import Task
from .schema import TopicsInputConfig
from .text import TextProcessor
from .util import DataclassJSONEncoder, InputIterator, ReaderFactory
from .util.file import count_lines, count_lines_with, path_append
from .util.formats import parse_xml_topics, parse_sgml_topics


@dataclasses.dataclass
class Topic:
    id: str
    lang: str
    title: str
    desc: Optional[str]
    narr: Optional[str]
    report: Optional[str]


@dataclasses.dataclass
class Query:
    id: str
    lang: str
    query: str  # string that may include query syntax for the retrieval engine
    text: str  # text that the query is based on
    report: Optional[str]


class TopicReaderFactory(ReaderFactory):
    classes = {
        'sgml': 'SgmlTopicReader',
        'xml': 'XmlTopicReader',
        'json': 'Hc4JsonTopicReader',
        'jsonl': 'Hc4JsonTopicReader',
        'msmarco': 'TsvTopicReader'
    }
    config_class = TopicsInputConfig
    name = 'topic type'


class TopicProcessor(Task):
    """Topic Preprocessing"""

    FIELD_MAP = {
        'title': 'title',
        'name': 'title',
        'desc': 'desc',
        'description': 'desc',
        'narr': 'narr',
        'narrative': 'narr'
    }

    def __init__(self, run_path, config):
        """
        Args:
            run_path (str): Root directory of the run.
            config (TopicsConfig)
        """
        super().__init__(run_path)
        self.fields = self._extract_fields(config.fields)

    def process(self, topic):
        """
        Args:
            topic (Topic)

        Returns
            Query
        """
        text = ' '.join([getattr(topic, f).strip() for f in self.fields])
        return Query(topic.id, topic.lang, text, text, topic.report)

    @classmethod
    def _extract_fields(cls, fields_str):
        fields = fields_str.split('+')
        try:
            return [cls.FIELD_MAP[f.lower()] for f in fields]
        except KeyError as e:
            raise ConfigError(f"Unrecognized topic field: {e}")


class SgmlTopicReader(InputIterator):
    """Iterator over topics from trec sgml"""

    def __init__(self, path, encoding, lang, prefix, strip_non_digits, **kwargs):
        self.path = path
        self.encoding = encoding
        self.lang = lang
        self.strip_non_digits = strip_non_digits
        self.topics = iter(topic for topic in parse_sgml_topics(path, encoding, prefix))

    def __iter__(self):
        return self

    def __next__(self):
        topic = next(self.topics)
        identifier = ''.join(filter(str.isdigit, topic[0])) if self.strip_non_digits else topic[0]
        return Topic(identifier, self.lang, topic[1], topic[2], topic[3], None)

    def __len__(self):
        return count_lines_with('<top>', self.path, self.encoding)


class XmlTopicReader(InputIterator):
    """Iterator over topics from trec xml"""

    def __init__(self, path, encoding, lang, strip_non_digits, **kwargs):
        self.path = path
        self.encoding = encoding
        self.lang = lang
        self.strip_non_digits = strip_non_digits
        self.topics = iter(topic for topic in parse_xml_topics(path, encoding))

    def __iter__(self):
        return self

    def __next__(self):
        topic = next(self.topics)
        identifier = ''.join(filter(str.isdigit, topic[0])) if self.strip_non_digits else topic[0]
        return Topic(identifier, topic[1], topic[2], topic[3], topic[4], None)

    def __len__(self):
        return count_lines_with('<topic', self.path, self.encoding)


class Hc4JsonTopicReader(InputIterator):
    """Iterator over topics from jsonl file """

    def __init__(self, path, encoding, lang, **kwargs):
        """
        Args:
            path (str): Path to topics file.
            encoding (str): File encoding.
            lang (str): Language of the topics.
            **kwargs (dict): Unused
        """
        self.path = path
        self.encoding = encoding
        self.lang = lang
        self.topics = iter(self._parse(path, encoding))

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.topics)

    def __len__(self):
        return count_lines(self.path, self.encoding)

    def _construct(self, data):
        # the default language in a topics file is always English
        try:
            if self.lang != "eng":
                if self.lang not in data['lang_supported']:
                    raise ConfigError(f"Language {self.lang} not supported in {self.path}")
                title = data['lang_resources'][self.lang]['topic_title'].strip()
                desc = data['lang_resources'][self.lang]['topic_description'].strip()
            else:
                title = data['topic_title'].strip()
                desc = data['topic_description'].strip()
            return Topic(data['topic_id'], self.lang, title, desc, None, data['report_text'])
        except KeyError as e:
            raise ParseError(f"Missing field {e} in json docs element: {data}")

    def _parse(self, path, encoding='utf8'):
        with open(path, 'r', encoding=encoding) as fp:
            try:
                return [self._construct(json.loads(data)) for data in fp]
            except json.decoder.JSONDecodeError as e:
                raise ParseError(f"Problem parsing json from {path}: {e}")


class TsvTopicReader(InputIterator):
    """Iterator over topics from tsv file like MSMARCO"""

    def __init__(self, path, encoding, lang, **kwargs):
        self.path = path
        self.encoding = encoding
        self.lang = lang
        self.topics = iter(topic for topic in self.parse(path, encoding))

    def __iter__(self):
        return self

    def __next__(self):
        topic = next(self.topics)
        return Topic(topic[0], self.lang, topic[1], None, None, None)

    def __len__(self):
        return count_lines(self.path, self.encoding)

    @staticmethod
    def parse(path, encoding='utf8'):
        with open(path, 'r', encoding=encoding) as fp:
            reader = csv.reader(fp, delimiter='\t')
            for line in reader:
                yield line[0], line[1].strip()


class QueryWriter(Task):
    """Write queries to a jsonl file using internal format"""

    def __init__(self, run_path, config, artifact_config):
        """
        Args:
            run_path (str): Root directory of the run.
            config (TopicsConfig or QueriesConfig): Config that includes output.
            artifact_config (BaseConfig or None): Config that resulted in this artifact
        """
        super().__init__(run_path, artifact_config, base=config.output)
        path = self.base / 'queries.jsonl'
        self.file = open(path, 'w')

    def process(self, query):
        """
        Args:
            query (Query)

        Returns
            Query
        """
        self.file.write(json.dumps(query, cls=DataclassJSONEncoder) + "\n")
        return query

    def end(self):
        super().end()
        self.file.close()

    def reduce(self, dirs):
        for base in dirs:
            path = path_append(base, 'queries.jsonl')
            with open(path) as fp:
                for line in fp:
                    self.file.write(line)


class QueryReader(InputIterator):
    """Iterator over queries from jsonl file """

    def __init__(self, path):
        self.path = pathlib.Path(path)
        if self.path.is_dir():
            self.path = self.path / 'queries.jsonl'
        with open(self.path) as fp:
            self.data = fp.readlines()

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return Query(**json.loads(self.data.pop(0)))
        except IndexError:
            raise StopIteration()

    def __len__(self):
        return count_lines(self.path)

    def peek(self):
        return Query(**json.loads(self.data[0]))


class QueryGenerator:
    def __init__(self, query, text, tokens, normalizer):
        self.id = query.id
        self.lang = query.lang
        self.text = text
        self.report = query.report
        self.original_query = query
        self.tokens = tokens
        self.normalizer = normalizer

    def generate(self):
        query = self.normalizer.post_normalize(' '.join(self.tokens))
        return Query(self.id, self.lang, query, self.text, self.report)


class ProbabilisticStructuredQueryGenerator(QueryGenerator):
    # TODO load translation table and generate PSQ syntax
    def __init__(self, query, text, tokens, normalizer, table_path):
        super().__init__(query, text, tokens, normalizer)

    def generate(self):
        query = self.normalizer.post_normalize(' '.join(self.tokens))
        return Query(self.id, self.lang, query, self.text, self.report)


class QueryProcessor(TextProcessor):
    """Query Preprocessing"""

    def __init__(self, run_path, config, lang):
        """
        Args:
            run_path (str): Root directory of the run.
            config (QueriesConfig)
            lang (str): Language code
        """
        super().__init__(run_path, config.process, lang)
        self.psq_path = config.psq

    def process(self, query):
        """
        Args:
            query (Query)

        Returns
            Query
        """
        text = query.text
        text = self.pre_normalize(text)
        tokens = self.tokenize(text)
        if self.psq_path:
            generator = ProbabilisticStructuredQueryGenerator(query, text, tokens, self.normalizer, self.psq_path)
        else:
            generator = QueryGenerator(query, text, tokens, self.normalizer)
        if self.run_lowercase:
            generator.tokens = self.lowercase(generator.tokens)
        stopword_indices = self.identify_stop_words(generator.tokens, self.run_lowercase)
        generator.tokens = self.stem(generator.tokens)
        generator.tokens = self.remove_stop_words(generator.tokens, stopword_indices)
        return generator.generate()
