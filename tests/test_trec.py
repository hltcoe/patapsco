import pathlib

import pytest

from pipeline.util.trec import *


def test_parse_documents():
    directory = pathlib.Path('.') / 'tests' / 'trec_files'
    path = directory / 'docs1.sgml'
    doc_iter = parse_documents(str(path.absolute()))
    doc = next(doc_iter)
    assert doc[0] == 'ABCDEF'
    assert doc[1].startswith('Aliens learn to code 20200601 Aliens learn to code using the Java')
    doc = next(doc_iter)
    assert doc[0] == 'TUVXYZ'
    assert doc[1].endswith('or even an asteroid strike.')
    with pytest.raises(StopIteration):
        next(doc_iter)


def test_parse_topics():
    directory = pathlib.Path('.') / 'tests' / 'trec_files'
    path = directory / 'topics.txt'
    topic_iter = parse_topics(str(path))
    topic = next(topic_iter)
    assert topic[0] == 'C141'
    assert topic[1] == 'Mating habits of robins'
    assert topic[2].startswith('Find information on the mating habits')
    assert topic[3].endswith('lay eggs in a nest.')
    topic = next(topic_iter)
    assert topic[0] == 'C142'
    assert topic[1] == 'Planting peas in the garden'
    assert topic[2].startswith('Find reports on the best conditions')
    assert topic[3].endswith('than many vegetables.')
    with pytest.raises(StopIteration):
        next(topic_iter)


def test_parse_qrels():
    directory = pathlib.Path('.') / 'tests' / 'trec_files'
    path = directory / 'qrels_2020'
    qrels_iter = parse_qrels(str(path))
    qrels = next(qrels_iter)
    assert len(qrels['141']) == 3
    assert qrels['141']['doc1'] == 0
    assert qrels['141']['doc2'] == 1
    assert qrels['141']['doc3'] == 0
    assert qrels['142']['doc1'] == 1
    assert qrels['142']['doc2'] == 0
    assert qrels['142']['doc3'] == 0
    with pytest.raises(StopIteration):
        next(qrels_iter)
