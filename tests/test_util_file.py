import pathlib
import tempfile

import pytest

from patapsco.error import ConfigError
from patapsco.util import file


def test_create_path_with_tilde():
    path = file.create_path("~/test")
    assert pathlib.Path.home() / "test" == path


def test_create_path_no_tilde():
    path = file.create_path("/exp")
    assert pathlib.Path("/exp") == path


def test_validate_encoding():
    file.validate_encoding('utf-8')
    file.validate_encoding('utf8')
    file.validate_encoding('ISO-8859-1')
    with pytest.raises(ConfigError):
        file.validate_encoding('abc')


def test_is_dir_empty_with_non_empty_dir():
    directory = pathlib.Path(__file__).parent / 'trec_files'
    assert not file.is_dir_empty(directory)


def test_is_dir_empty_with_empty_dir():
    directory = tempfile.mkdtemp()
    assert file.is_dir_empty(directory)
    file.delete_dir(directory)


def test_count_lines():
    directory = pathlib.Path(__file__).parent / 'trec_files'
    assert file.count_lines(str(directory / 'hamshahri_docs.txt')) == 10
    assert file.count_lines(str(directory / 'results.txt')) == 4


def test_count_lines_with():
    directory = pathlib.Path(__file__).parent / 'trec_files'
    assert file.count_lines_with('.DID', str(directory / 'hamshahri_docs.txt')) == 2
    assert file.count_lines_with('aaa', str(directory / 'results.txt')) == 2
