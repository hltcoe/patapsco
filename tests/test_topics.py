import pathlib
import tempfile

import pytest

from patapsco.topics import *

from patapsco.util.file import delete_dir
from patapsco.schema import PSQConfig, TextProcessorConfig, QueriesConfig
from patapsco.text import TextProcessor
from patapsco.topics import PSQGenerator, Query


def test_topic_process():
    class Mock:
        def __init__(self, fields):
            self.fields = fields

    mock = Mock(['title', 'desc'])
    topic = Topic('1', 'eng', 'title', 'desc', 'narr', 'report')
    query = TopicProcessor.process(mock, topic)
    assert query.text == "title desc"


def test_extract_fields_with_case():
    fields_str = 'title+DESC'
    fields = TopicProcessor._extract_fields(fields_str)
    assert fields == ['title', 'desc']


def test_extract_fields_with_bad_field():
    fields_str = 'title+report'
    with pytest.raises(ConfigError):
        TopicProcessor._extract_fields(fields_str)


def test_parse_msmarco_topics():
    directory = pathlib.Path(__file__).parent / 'msmarco_files'
    path = directory / 'queries.tsv'
    topic_iter = TsvTopicReader.parse(str(path.absolute()))
    topic = next(topic_iter)
    assert topic[0] == '1'
    assert topic[1] == 'is this a test?'
    topic = next(topic_iter)
    assert topic[0] == '2'
    assert topic[1] == 'define test'
    with pytest.raises(StopIteration):
        next(topic_iter)


class TestHc4JsonTopicReader:
    def test_parse_json_topics(self):
        directory = pathlib.Path(__file__).parent / 'json_files'
        path = directory / 'topics.jsonl'
        topic_iter = Hc4JsonTopicReader(str(path.absolute()), 'utf8', 'eng')
        topic = next(topic_iter)
        assert topic.id == '001'
        assert topic.title == 'Test 1'
        assert topic.desc == 'First test'
        assert topic.report == 'report 1'
        topic = next(topic_iter)
        assert topic.id == '002'
        assert topic.title == 'Test 2'
        assert topic.desc == 'Second test'
        assert topic.report == 'report 2'
        with pytest.raises(StopIteration):
            next(topic_iter)

    def test_with_bad_language(self):
        directory = pathlib.Path(__file__).parent / 'json_files'
        path = directory / 'topics.jsonl'
        with pytest.raises(ConfigError):
            Hc4JsonTopicReader(str(path.absolute()), 'utf8', 'spa')

    def test_parse_json_topics_lang_resources(self):
        directory = pathlib.Path(__file__).parent / 'json_files'
        path = directory / 'topics.jsonl'
        topic_iter = Hc4JsonTopicReader(str(path.absolute()), 'utf8', 'rus')
        topic = next(topic_iter)
        assert topic.id == '001'
        assert topic.title == 'Тест 1'
        assert topic.desc == 'Первый тест'
        assert topic.report == 'report 1'
        topic = next(topic_iter)
        assert topic.id == '002'
        assert topic.title == 'Тест 2'
        assert topic.desc == 'Второй тест'
        assert topic.report == 'report 2'
        with pytest.raises(StopIteration):
            next(topic_iter)

    def test_parse_json_topics_filter_by_lang(self):
        directory = pathlib.Path(__file__).parent / 'json_files'
        path = directory / 'topics_different_langs.jsonl'
        topic_iter = Hc4JsonTopicReader(str(path.absolute()), 'utf8', 'zho')
        topic = next(topic_iter)
        assert topic.id == '001'
        assert topic.title == '测试1'
        assert topic.desc == '第一次测试'
        assert topic.report == 'report 1'
        with pytest.raises(StopIteration):
            next(topic_iter)

    def test_parse_json_topics_with_none_primary_lang(self):
        directory = pathlib.Path(__file__).parent / 'json_files'
        path = directory / 'topics_none.jsonl'
        topic_iter = Hc4JsonTopicReader(str(path.absolute()), 'utf8', 'eng')
        topic = next(topic_iter)
        assert topic.id == '002'

    def test_parse_json_topics_with_none_resources_lang(self):
        directory = pathlib.Path(__file__).parent / 'json_files'
        path = directory / 'topics_none.jsonl'
        topic_iter = Hc4JsonTopicReader(str(path.absolute()), 'utf8', 'zho')
        topic = next(topic_iter)
        assert topic.id == '002'

    def test_parse_json_topics_with_filter(self):
        directory = pathlib.Path(__file__).parent / 'json_files'
        path = directory / 'topics_filter.jsonl'
        topic_iter = Hc4JsonTopicReader(str(path.absolute()), 'utf8', 'eng', 'zho')
        topic = next(topic_iter)
        assert topic.id == '002'

    def test_parse_json_topics_with_filter2(self):
        directory = pathlib.Path(__file__).parent / 'json_files'
        path = directory / 'topics_filter.jsonl'
        topic_iter = Hc4JsonTopicReader(str(path.absolute()), 'utf8', 'eng', 'rus')
        topic = next(topic_iter)
        assert topic.id == '001'
        topic = next(topic_iter)
        assert topic.id == '002'

    def test_parse_json_topics_with_lang(self):
        directory = pathlib.Path(__file__).parent / 'json_files'
        path = directory / 'topics_new_lang_field.jsonl'
        topic_iter = Hc4JsonTopicReader(str(path.absolute()), 'utf8', 'rus')
        topic = next(topic_iter)
        assert topic.id == '001'
        topic = next(topic_iter)
        assert topic.id == '002'

    def test_parse_json_topics_with_lang_misatch(self):
        directory = pathlib.Path(__file__).parent / 'json_files'
        path = directory / 'topics_new_lang_field.jsonl'
        with pytest.raises(ConfigError):
            Hc4JsonTopicReader(str(path.absolute()), 'utf8', 'eng')


def test_query_reader():
    directory = pathlib.Path(__file__).parent / 'json_files'
    query_iter = QueryReader(str(directory))
    query = next(query_iter)
    assert query.id == '001'
    assert query.lang == 'eng'
    assert query.text == 'test 1'
    assert query.report == 'report 1'
    query = next(query_iter)
    assert query.id == '002'
    assert query.lang == 'eng'
    assert query.text == 'test 2'
    assert query.report == 'report 2'
    with pytest.raises(StopIteration):
        next(query_iter)

class TestPSQ:
    def setup_method(self):
        self.temp_dir = pathlib.Path(tempfile.mkdtemp())

    def teardown_method(self):
        delete_dir(self.temp_dir)

    def test_psq_query_generation(self):
        directory = pathlib.Path(__file__).parent / 'psq_files'
        path = directory / 'psq.json'
        text_config = TextProcessorConfig(
            tokenize="whitespace",
            stopwords=False,
            stem=False
        )
        processor = TextProcessor(self.temp_dir, text_config, 'eng')
        processor.begin()  # load models
        generator = PSQGenerator(processor, path, 0.97)
        query = generator.generate(Query(1, '', '', '', ''), '', ['cat', 'dog', 'bird', 'hello'])
        assert query.query == "psq AND (gato^0.8421 felino^0.1579) AND (pero^0.8182 can^0.1818) AND (pájaro^0.6122 ave^0.3878) AND (hola^1.0000)"

    def test_psq_generation_remove_empty_entry(self):
        directory = pathlib.Path(__file__).parent / 'psq_files'
        path = directory / 'psq_table.json'
        text_config = TextProcessorConfig(
            tokenize="whitespace",
            stopwords=False,
            stem=False
        )
        processor = TextProcessor(self.temp_dir, text_config, 'eng')
        processor.begin()  # load models
        generator = PSQGenerator(processor, path, 0.97)
        query = generator.generate(Query(1, '', '', '', ''), '', ['cat', 'error'])
        assert query.query == "psq AND (gato^0.8421 felino^0.1579)"


def test_lucene_query_transformer():
    text_config = TextProcessorConfig(
        tokenize="whitespace",
        stopwords=False,
        stem="porter"
    )
    processor = TextProcessor('', text_config, 'eng')
    processor.begin()
    stemmer = LuceneTransformer(processor)
    tree = luqum.parser.parser.parse("contents:running AND contents:wonderful")
    assert str(stemmer.visit(tree)) == "contents:run AND contents:wonder"


class TestLuceneQueryGenerator:
    def setup_method(self):
        text_config = TextProcessorConfig(
            tokenize="whitespace",
            stopwords=False,
            stem="porter"
        )
        self.processor = TextProcessor('', text_config, 'eng')
        self.processor.begin()
        self.gen = LuceneQueryGenerator(self.processor)

    def process(self, text):
        return self.gen.generate(Query('1', '', '', '', ''), text, [])

    def test_not_stemming(self):
        text_config = TextProcessorConfig(
            tokenize="whitespace",
            stopwords=False,
            stem=False
        )
        processor = TextProcessor('', text_config, 'eng')
        processor.begin()
        gen = LuceneQueryGenerator(processor)
        query = gen.generate(Query('1', '', '', '', ''), 'running AND wonderful', [])
        assert query.query == '+contents:running +contents:wonderful'

    def test_and(self):
        query = self.process('running AND wonderful')
        assert query.query == '+contents:run +contents:wonder'

    def test_or(self):
        query = self.process('running OR wonderful')
        assert query.query == 'contents:run contents:wonder'

    def test_not(self):
        query = self.process('running NOT wonderful')
        assert query.query == 'contents:run -contents:wonder'

    def test_phrase(self):
        query = self.process('"running wonderful"')
        assert query.query == 'contents:"run wonder"'

    def test_grouping(self):
        query = self.process('(running OR walking) AND relaxing')
        assert query.query == '+(contents:run contents:walk) +contents:relax'


class TestQueryProcessor:
    def test_config_conflict(self):
        config = QueriesConfig(process=TextProcessorConfig(tokenize="whitespace"),
                               psq=PSQConfig(path='', lang='eng'),
                               parse=True)
        with pytest.raises(ConfigError):
            QueryProcessor('', config, 'eng')
