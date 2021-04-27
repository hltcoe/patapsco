import pathlib

import pytest

from patapsco.util import *


def test_chunked_iterator_divisible():
    data = [1, 2, 3, 4]
    it = ChunkedIterator(iter(data), 2)
    assert next(it) == [1, 2]
    assert next(it) == [3, 4]
    with pytest.raises(StopIteration):
        next(it)


def test_chunked_iterator_not_divisible():
    data = [1, 2, 3, 4]
    it = ChunkedIterator(iter(data), 3)
    assert next(it) == [1, 2, 3]
    assert next(it) == [4]
    with pytest.raises(StopIteration):
        next(it)


def test_chunked_iterator_all():
    data = [1, 2, 3, 4]
    it = ChunkedIterator(iter(data), None)
    assert next(it) == [1, 2, 3, 4]
    with pytest.raises(StopIteration):
        next(it)


class MockIterator:
    def __init__(self, path):
        self.path = path
        with open(path) as fp:
            self.values = iter([line.strip() for line in fp])

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.values)

    def __len__(self):
        return file.count_lines(self.path)


class TestGlobIterator:
    def test_with_absolute(self):
        directory = pathlib.Path(__file__).parent / 'glob_files'
        glob = directory / 'other_file.txt'
        iterator = GlobIterator(str(glob.absolute()), MockIterator)
        assert next(iterator) == '4'
        assert next(iterator) == '5'
        with pytest.raises(StopIteration):
            next(iterator)

    def test_with_relative(self):
        directory = pathlib.Path(__file__).parent / 'glob_files'
        glob = directory / 'other_file.txt'
        iterator = GlobIterator(str(glob), MockIterator)
        assert next(iterator) == '4'

    def test_with_bad_path(self):
        directory = pathlib.Path(__file__).parent / 'glob_files'
        glob = directory / 'nothing.txt'
        with pytest.raises(ConfigError):
            GlobIterator(str(glob.absolute()), MockIterator)

    def test_with_bad_path_not_first(self):
        directory = pathlib.Path(__file__).parent / 'glob_files'
        glob1 = directory / 'file?.txt'
        glob2 = directory / 'nothing.txt'
        with pytest.raises(ConfigError):
            GlobIterator([str(glob1), str(glob2)], MockIterator)

    def test_with_multiple_patterns(self):
        directory = pathlib.Path(__file__).parent / 'glob_files'
        glob1 = directory / 'file?.txt'
        glob2 = directory / 'other*'
        iterator = GlobIterator([str(glob1), str(glob2)], MockIterator)
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
        iterator = GlobIterator(str(glob.absolute()), bad_input)
        with pytest.raises(BadDataError):
            next(iterator)

    def test_len(self):
        directory = pathlib.Path(__file__).parent / 'glob_files'
        glob = directory / 'file?.txt'
        iterator = GlobIterator(str(glob), MockIterator)
        assert len(iterator) == 3


class TestSlicedIterator:
    def test_len(self):
        directory = pathlib.Path(__file__).parent / 'glob_files'
        glob1 = directory / 'file?.txt'
        glob2 = directory / 'other*'
        it1 = GlobIterator([str(glob1), str(glob2)], MockIterator)
        it2 = SlicedIterator(it1, 0, 5)
        assert len(it1) == 5
        assert len(it2) == 5
        it2 = SlicedIterator(it1, 0, 4)
        assert len(it2) == 4
        it2 = SlicedIterator(it1, 0, 10)
        assert len(it2) == 5
        it2 = SlicedIterator(it1, 2, 4)
        assert len(it2) == 2

    def test_next(self):
        directory = pathlib.Path(__file__).parent / 'glob_files'
        glob1 = directory / 'file?.txt'
        glob2 = directory / 'other*'
        it1 = GlobIterator([str(glob1), str(glob2)], MockIterator)
        it2 = SlicedIterator(it1, 1, 4)
        assert next(it2) == '2'
        assert next(it2) == '3'
        assert next(it2) == '4'
        with pytest.raises(StopIteration):
            next(it2)

    def test_with_nones(self):
        directory = pathlib.Path(__file__).parent / 'glob_files'
        glob1 = directory / 'file?.txt'
        glob2 = directory / 'other*'
        it1 = GlobIterator([str(glob1), str(glob2)], MockIterator)
        it2 = SlicedIterator(it1, None, None)
        assert next(it2) == '1'
        assert next(it2) == '2'
        assert next(it2) == '3'
        assert next(it2) == '4'
        assert next(it2) == '5'
        with pytest.raises(StopIteration):
            next(it2)

    def test_with_none_start(self):
        directory = pathlib.Path(__file__).parent / 'glob_files'
        glob1 = directory / 'file?.txt'
        glob2 = directory / 'other*'
        it1 = GlobIterator([str(glob1), str(glob2)], MockIterator)
        it2 = SlicedIterator(it1, None, 2)
        assert next(it2) == '1'
        assert next(it2) == '2'
        with pytest.raises(StopIteration):
            next(it2)

    def test_with_none_stop(self):
        directory = pathlib.Path(__file__).parent / 'glob_files'
        glob1 = directory / 'file?.txt'
        glob2 = directory / 'other*'
        it1 = GlobIterator([str(glob1), str(glob2)], MockIterator)
        it2 = SlicedIterator(it1, 3, None)
        assert next(it2) == '4'
        assert next(it2) == '5'
        with pytest.raises(StopIteration):
            next(it2)
