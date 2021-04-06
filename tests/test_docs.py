import pathlib

import pytest

from pipeline.docs import *


def test_parse_json_documents():
    directory = pathlib.Path('.') / 'tests' / 'json_files'
    path = directory / 'docs.jsonl'
    doc_iter = parse_json_documents(str(path.absolute()))
    doc = next(doc_iter)
    assert doc[0] == 'abcdef'
    assert doc[1] == 'title1 text1'
    doc = next(doc_iter)
    assert doc[0] == 'tuvwxy'
    assert doc[1] == 'title2 text2'
    with pytest.raises(StopIteration):
        next(doc_iter)
