import pathlib

import pytest

from patapsco.docs import *


def test_parse_json_documents():
    directory = pathlib.Path(__file__).parent / 'json_files'
    path = directory / 'docs.jsonl'
    doc_iter = Tc4JsonDocumentReader.parse(str(path.absolute()))
    doc = next(doc_iter)
    assert doc[0] == 'abcdef'
    assert doc[1] == 'title1 text1'
    doc = next(doc_iter)
    assert doc[0] == 'tuvwxy'
    assert doc[1] == 'title2 text2'
    with pytest.raises(StopIteration):
        next(doc_iter)


def test_parse_json_documents_with_bad_format():
    directory = pathlib.Path(__file__).parent / 'json_files'
    path = directory / 'bad_format.jsonl'
    doc_iter = Tc4JsonDocumentReader.parse(str(path.absolute()))
    with pytest.raises(ParseError):
        next(doc_iter)


def test_parse_json_documents_with_missing_field():
    directory = pathlib.Path(__file__).parent / 'json_files'
    path = directory / 'missing_field.jsonl'
    doc_iter = Tc4JsonDocumentReader.parse(str(path.absolute()))
    with pytest.raises(ParseError):
        next(doc_iter)


def test_parse_msmarco_documents():
    directory = pathlib.Path(__file__).parent / 'msmarco_files'
    path = directory / 'collection.tsv'
    doc_iter = TsvDocumentReader.parse(str(path.absolute()))
    doc = next(doc_iter)
    assert doc[0] == '1'
    assert doc[1] == 'mary had a little lamb'
    doc = next(doc_iter)
    assert doc[0] == '2'
    assert doc[1] == 'four score and seven years ago'
    with pytest.raises(StopIteration):
        next(doc_iter)
