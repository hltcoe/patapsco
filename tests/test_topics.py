import pathlib

import pytest

from pipeline.topics import *


def test_select_text():
    class Mock:
        def __init__(self, fields):
            self.fields = fields

    mock = Mock(['title', 'desc'])
    topic = Topic('1', 'en', 'title', 'desc', 'narr')
    text = TopicProcessor._select_text(mock, topic)
    assert text == "title desc"


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
