import csv
import dataclasses
import json
import logging
import pathlib
from typing import Optional

import luqum.parser
import luqum.tree
import luqum.visitor

from .error import ConfigError, ParseError
from .pipeline import Task
from .schema import TextProcessorConfig, TopicsInputConfig
from .text import TextProcessor
from .util import DataclassJSONEncoder, InputIterator, ReaderFactory
from .util.file import count_lines, count_lines_with, path_append
from .util.formats import parse_xml_topics, parse_sgml_topics, parse_psq_table
from .util.java import Java

LOGGER = logging.getLogger(__name__)


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
    text: str  # original text that the query is based on
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
        if not text:
            LOGGER.warning(f"Query from topic {topic.id} has no text.")
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


class SkipEntry(Exception):
    # Indicates that a topic should be skipped
    pass


class Hc4JsonTopicReader(InputIterator):
    """Iterator over topics from jsonl file """

    def __init__(self, path, encoding, lang, filter_lang=None, **kwargs):
        """
        Args:
            path (str): Path to topics file.
            encoding (str): File encoding.
            lang (str): Language of the topics.
            filter_lang (str): Remove topics that do not have this lang in lang_supported
            **kwargs (dict): Unused
        """
        self.path = path
        self.encoding = encoding
        self.lang = lang
        self.filter_lang = filter_lang
        self.num_skipped = 0
        self.topics = iter(self._parse(path, encoding))

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.topics)

    def __len__(self):
        return count_lines(self.path, self.encoding)

    def _construct(self, data):
        # if language is not specified, assume it is English
        primary_lang = data['lang'] if 'lang' in data else 'eng'
        try:
            if self.filter_lang and self.filter_lang not in data['lang_supported']:
                raise SkipEntry()
            if self.lang == primary_lang:
                self._validate(data['topic_id'], data)
                title = data['topic_title'].strip()
                desc = data['topic_description'].strip()
            else:
                if self.lang not in data['lang_supported']:
                    raise SkipEntry()
                self._validate(data['topic_id'], data['lang_resources'][self.lang])
                title = data['lang_resources'][self.lang]['topic_title'].strip()
                desc = data['lang_resources'][self.lang]['topic_description'].strip()
            return Topic(data['topic_id'], self.lang, title, desc, None, data['report_text'])
        except SkipEntry:
            self.num_skipped += 1
            return None
        except KeyError as e:
            raise ParseError(f"Missing field {e} in json docs element: {data}")

    def _validate(self, topic_id, data):
        # None is not allowed for title or description
        if data['topic_title'] is None or data['topic_description'] is None:
            LOGGER.warning(f"Skipping topic {topic_id} because of null title or description")
            raise SkipEntry()

    def _parse(self, path, encoding='utf8'):
        with open(path, 'r', encoding=encoding) as fp:
            try:
                topics = [self._construct(json.loads(data)) for data in fp]
                # filter topics that are not supported for this language or have errors
                topics = [topic for topic in topics if topic is not None]
                if self.num_skipped:
                    LOGGER.info(f"Skipping {self.num_skipped} topics not supported for {self.lang}")
                if not topics:
                    raise ConfigError(f"No topics available for language {self.lang}")
                return topics
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
    """Generate a plain text query"""

    def __init__(self, processor):
        self.processor = processor

    def generate(self, query, text, tokens):
        """Generate the text of a query

        Args:
            query (Query): query object
            text (str): normalized text of query
            tokens (list): list of string tokens

        Returns:
            Query object
        """
        stopword_indices = self.processor.identify_stop_words(tokens)
        tokens = self.processor.stem(tokens)
        tokens = self.processor.remove_stop_words(tokens, stopword_indices)
        query_syntax = self.processor.post_normalize(' '.join(tokens))
        return Query(query.id, query.lang, query_syntax, text, query.report)


class LuceneTransformer(luqum.visitor.TreeTransformer):
    """Applies text processing to the terms in a Lucene query"""

    def __init__(self, processor):
        self.processor = processor
        super().__init__()

    def visit_search_field(self, node, context):
        new_node = node.clone_item()
        value = node.expr.value
        if isinstance(node.expr, luqum.tree.Phrase):
            value = value.strip('"')
        # this handles single terms and phrases
        tokens = value.split()
        stopword_indices = self.processor.identify_stop_words(tokens)
        tokens = self.processor.stem(tokens)
        tokens = self.processor.remove_stop_words(tokens, stopword_indices)
        new_value = self.processor.post_normalize(' '.join(tokens))
        if isinstance(node.expr, luqum.tree.Phrase):
            new_value = f'"{new_value}"'
        new_node.expr = node.expr.clone_item(value=new_value)
        yield new_node


class LuceneQueryGenerator:
    """Generate a query using Lucene syntax"""

    def __init__(self, processor):
        self.processor = processor
        java = Java()
        self.parser = java.QueryParser('contents', java.WhitespaceAnalyzer())
        self.transformer = LuceneTransformer(processor)

    def generate(self, query, text, tokens):
        """Generate the text of a query

        Args:
            query (Query): query object
            text (str): normalized text of query
            tokens (list): list of string tokens

        Returns:
            Query object
        """
        # convert tokens into lucene query format:
        # term1 AND term2 -> +contents:term1 +contents:term2
        jquery = self.parser.parse(text)
        tree = luqum.parser.parser.parse(jquery.toString())

        # stem, remove stop words, normalize the query terms
        tree = self.transformer.visit(tree)

        return Query(query.id, query.lang, str(tree), text, query.report)


@dataclasses.dataclass
class PSQToken:
    text: str
    prob: Optional[float]


class PSQGenerator(QueryGenerator):
    """Generate a PSQ"""
    def __init__(self, processor, psq_path, threshold):
        super().__init__(processor)
        try:
            self.psq_table = parse_psq_table(psq_path, threshold)
        except OSError as e:
            raise ConfigError(f"Unable to load PSQ translation table: {e}")

    def generate(self, query, text, tokens):
        """Post process the tokens (stem, stop words, normalize) and generate PSQ"""
        psq_tokens = self._project(token.lower() for token in tokens)

        terms = [' '.join(self.process_psq(psq_clause)) for psq_clause in psq_tokens]
        query_syntax = ') AND ('.join([self.processor.post_normalize(term) for term in terms if term != ''])
        return Query(query.id, query.lang, 'psq AND (' + query_syntax + ')', text, query.report)

    def process_psq(self, psq_tokens):
        # remove stop words and stem and apply to PSQ tokens
        text_tokens = [token.text for token in psq_tokens]
        stopword_indices = self.processor.identify_stop_words(text_tokens, is_lower=True)
        text_tokens = self.processor.stem(text_tokens)
        for index in range(len(psq_tokens)):
            psq_tokens[index].text = text_tokens[index]
        psq_tokens = self.processor.remove_stop_words(psq_tokens, stopword_indices)
        # normalize the text of the PSQ tokens and remove those that are now empty
        for psq_token in psq_tokens:
            psq_token.text = self.processor.post_normalize(psq_token.text)
        psq_tokens = [psq_token for psq_token in psq_tokens if psq_token.text]
        # formulate the query syntax for weighted query
        terms = [self._format_term(psq_token) for psq_token in psq_tokens]
        return terms

    # These characters have special meaning in Lucene so if we want to use them literally they need to be escaped
    def escape_term(self, term):
        return term.translate(str.maketrans({"-": r"\-",
                                             "]": r"\]",
                                             "[": r"\[",
                                             "+": r"\+",
                                             "|": r"\|",
                                             "!": r"\!",
                                             "(": r"\(",
                                             ")": r"\)",
                                             "}": r"\}",
                                             "{": r"\{",
                                             "/": r"\/",
                                             "\"": r"\\\"",
                                             "~": r"\~",
                                             "?": r"\?",
                                             "\\": r"\\",
                                             "^": r"\^",
                                             "*": r"\*",
                                             "&": r"\&",
                                             ":": r"\:"}))

    def _format_term(self, psq_token):
        """PSQ syntax with Lucene boost syntax"""
        if psq_token.prob:
            return f"{self.escape_term(psq_token.text)}^{psq_token.prob:.4f}"
        else:
            return f"{self.escape_term(psq_token.text)}^{1.0}"

    def _project(self, tokens):
        """project the query into the target language"""
        eng_tokens = []
        for token in tokens:
            if token in self.psq_table:
                eng_tokens.append([PSQToken(text, prob) for text, prob in self.psq_table[token].items()])
            else:
                eng_tokens.append([PSQToken(token, None)])
        return eng_tokens


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
        self.psq_config = config.psq
        self.parse = config.parse
        if self.psq_config and self.parse:
            raise ConfigError("Cannot use both PSQ and Lucene query parsing")
        self.generator = None

    def begin(self):
        super().begin()
        if self.psq_config:
            # build an English text processor to handle English tokens from PSQ
            text_config = TextProcessorConfig(
                normalize=self.psq_config.normalize,
                tokenize="whitespace",
                stopwords=self.psq_config.stopwords,
                stem=self.psq_config.stem
            )
            processor = TextProcessor(self.run_path, text_config, self.psq_config.lang)
            processor.begin()  # load models
            self.generator = PSQGenerator(processor, self.psq_config.path, self.psq_config.threshold)
        elif self.parse:
            self.generator = LuceneQueryGenerator(self)
        else:
            self.generator = QueryGenerator(self)

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
        return self.generator.generate(query, text, tokens)
