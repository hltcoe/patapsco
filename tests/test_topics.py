import pathlib

import pytest

from patapsco.topics import *


def test_select_text():
    class Mock:
        def __init__(self, fields):
            self.fields = fields

    mock = Mock(['title', 'desc'])
    topic = Topic('1', 'en', 'title', 'desc', 'narr')
    query = TopicProcessor.process(mock, topic)
    assert query.text == "title desc"


def test_parse_msmarco_topics():
    directory = pathlib.Path('.') / 'tests' / 'msmarco_files'
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
    directory = pathlib.Path('.') / 'tests' / 'json_files'
    path = directory / 'topics.jsonl'
    topic_iter = JsonTopicReader.parse(str(path.absolute()))
    topic = next(topic_iter)
    assert topic[0] == '001'
    assert topic[1] == 'Test 1'
    assert topic[2] == 'First test'
    topic = next(topic_iter)
    assert topic[0] == '002'
    assert topic[1] == 'Test 2'
    assert topic[2] == 'Second test'
    with pytest.raises(StopIteration):
        next(topic_iter)


def test_query_reader():
    directory = pathlib.Path('.') / 'tests' / 'json_files'
    query_iter = QueryReader(str(directory))
    query = next(query_iter)
    assert query.id == '001'
    assert query.lang == 'en'
    assert query.text == 'test 1'
    query = next(query_iter)
    assert query.id == '002'
    assert query.lang == 'en'
    assert query.text == 'test 2'
    with pytest.raises(StopIteration):
        next(query_iter)
