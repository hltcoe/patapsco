import pathlib

import pytest

from patapsco.topics import *


def test_topic_process():
    class Mock:
        def __init__(self, fields):
            self.fields = fields

    mock = Mock(['title', 'desc'])
    topic = Topic('1', 'en', 'title', 'desc', 'narr', 'report')
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


def test_parse_json_topics():
    directory = pathlib.Path(__file__).parent / 'json_files'
    path = directory / 'topics.jsonl'
    topic_iter = Hc4JsonTopicReader(str(path.absolute()), 'utf8', 'en')
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


def test_query_reader():
    directory = pathlib.Path(__file__).parent / 'json_files'
    query_iter = QueryReader(str(directory))
    query = next(query_iter)
    assert query.id == '001'
    assert query.lang == 'en'
    assert query.text == 'test 1'
    assert query.report == 'report 1'
    query = next(query_iter)
    assert query.id == '002'
    assert query.lang == 'en'
    assert query.text == 'test 2'
    assert query.report == 'report 2'
    with pytest.raises(StopIteration):
        next(query_iter)
