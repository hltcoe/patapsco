import pathlib

import pytest

from patapsco.results import *


def test_json_results_reader():
    directory = pathlib.Path(__file__).parent / 'json_files'
    results_iter = JsonResultsReader(str(directory))
    results = next(results_iter)
    assert results.query.id == '001'
    assert results.query.lang == 'eng'
    assert results.query.text == 'test 1'
    assert results.system == 'PyseriniRetriever'
    assert len(results.results) == 2
    assert results.results[0].doc_id == 'aaa'
    assert results.results[0].rank == 1
    results = next(results_iter)
    assert results.query.id == '002'
    assert results.query.lang == 'eng'
    assert results.query.text == 'test 2'
    assert results.system == 'PyseriniRetriever'
    assert len(results.results) == 2
    assert results.results[0].doc_id == 'bbb'
    assert results.results[0].rank == 1
    with pytest.raises(StopIteration):
        next(results_iter)


def test_trec_results_reader():
    path = pathlib.Path(__file__).parent / 'trec_files' / 'results.txt'
    results_iter = TrecResultsReader(str(path))
    results = next(results_iter)
    assert results.query.id == '1'
    assert results.system == 'MockReranker'
    assert len(results.results) == 2
    assert results.results[0].doc_id == 'aaa'
    assert results.results[0].rank == 1
    results = next(results_iter)
    assert results.query.id == '2'
    assert results.system == 'MockReranker'
    assert len(results.results) == 2
    assert results.results[0].doc_id == 'bbb'
    assert results.results[0].rank == 1
    with pytest.raises(StopIteration):
        next(results_iter)
