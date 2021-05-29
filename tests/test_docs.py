import pathlib

import pytest

from patapsco.docs import *


def test_parse_json_documents():
    directory = pathlib.Path(__file__).parent / 'json_files'
    path = directory / 'docs.jsonl'
    doc_iter = Hc4JsonDocumentReader(str(path.absolute()), 'utf8', 'eng')
    doc = next(doc_iter)
    assert doc.id == 'abcdef'
    assert doc.lang == 'eng'
    assert doc.text == 'title1 text1'
    assert doc.date == '2020-12-31'
    doc = next(doc_iter)
    assert doc.id == 'tuvwxy'
    assert doc.text == 'title2 text2'
    assert doc.date == '2020-12-31'
    with pytest.raises(StopIteration):
        next(doc_iter)
    assert doc_iter.fp.closed


def test_parse_json_documents_with_bad_format():
    directory = pathlib.Path(__file__).parent / 'json_files'
    path = directory / 'bad_format.jsonl'
    doc_iter = Hc4JsonDocumentReader(str(path.absolute()), 'utf8', 'eng')
    with pytest.raises(ParseError):
        next(doc_iter)


def test_parse_json_documents_with_missing_field():
    directory = pathlib.Path(__file__).parent / 'json_files'
    path = directory / 'missing_field.jsonl'
    doc_iter = Hc4JsonDocumentReader(str(path.absolute()), 'utf8', 'eng')
    with pytest.raises(ParseError):
        next(doc_iter)


def test_parse_msmarco_documents():
    directory = pathlib.Path(__file__).parent / 'msmarco_files'
    path = directory / 'collection.tsv'
    doc_iter = TsvDocumentReader(str(path.absolute()), 'utf8', 'eng')
    doc = next(doc_iter)
    assert doc.id == '1'
    assert doc.text == 'mary had a little lamb'
    doc = next(doc_iter)
    assert doc.id == '2'
    assert doc.text == 'four score and seven years ago'
    with pytest.raises(StopIteration):
        next(doc_iter)
    assert doc_iter.fp.closed
