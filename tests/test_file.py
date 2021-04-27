import pathlib

import pytest

from patapsco.error import BadDataError, ConfigError
from patapsco.util import file


def next_line(path):
    with open(path) as fp:
        for line in fp:
            yield line.strip()


def test_validate_encoding():
    file.validate_encoding('utf-8')
    file.validate_encoding('utf8')
    file.validate_encoding('ISO-8859-1')
    with pytest.raises(ConfigError):
        file.validate_encoding('abc')


def test_count_lines():
    directory = pathlib.Path(__file__).parent / 'trec_files'
    assert file.count_lines(str(directory / 'hamshahri_docs.txt')) == 10
    assert file.count_lines(str(directory / 'results.txt')) == 4


class TestGlobFileGenerator:
    def test_with_absolute(self):
        directory = pathlib.Path(__file__).parent / 'glob_files'
        glob = directory / 'other_file.txt'
        iterator = file.GlobFileGenerator(str(glob.absolute()), next_line)
        assert next(iterator) == '4'
        assert next(iterator) == '5'
        with pytest.raises(StopIteration):
            next(iterator)

    def test_with_relative(self):
        directory = pathlib.Path(__file__).parent / 'glob_files'
        glob = directory / 'other_file.txt'
        iterator = file.GlobFileGenerator(str(glob), next_line)
        assert next(iterator) == '4'

    def test_with_bad_path(self):
        directory = pathlib.Path(__file__).parent / 'glob_files'
        glob = directory / 'nothing.txt'
        with pytest.raises(ConfigError):
            file.GlobFileGenerator(str(glob.absolute()), next_line)

    def test_with_bad_path_not_first(self):
        directory = pathlib.Path(__file__).parent / 'glob_files'
        glob1 = directory / 'file?.txt'
        glob2 = directory / 'nothing.txt'
        with pytest.raises(ConfigError):
            file.GlobFileGenerator([str(glob1), str(glob2)], next_line)

    def test_with_multiple_patterns(self):
        directory = pathlib.Path(__file__).parent / 'glob_files'
        glob1 = directory / 'file?.txt'
        glob2 = directory / 'other*'
        iterator = file.GlobFileGenerator([str(glob1), str(glob2)], next_line)
        assert next(iterator) == '1'
        assert next(iterator) == '2'
        assert next(iterator) == '3'
        assert next(iterator) == '4'
        assert next(iterator) == '5'
        with pytest.raises(StopIteration):
            next(iterator)

    def test_with_bad_input_file(self):
        # bad input results in immediate StopIteration
        def bad_input(path):
            if False:
                yield '1', 'text'

        directory = pathlib.Path(__file__).parent / 'glob_files'
        glob = directory / 'file1.txt'
        iterator = file.GlobFileGenerator(str(glob.absolute()), bad_input)
        with pytest.raises(BadDataError):
            next(iterator)
