import pytest

from patapsco.util import *


def test_chunked_iterator_divisible():
    data = [1, 2, 3, 4]
    it = ChunkedIterable(iter(data), 2)
    assert next(it) == [1, 2]
    assert next(it) == [3, 4]
    with pytest.raises(StopIteration):
        next(it)


def test_chunked_iterator_not_divisible():
    data = [1, 2, 3, 4]
    it = ChunkedIterable(iter(data), 3)
    assert next(it) == [1, 2, 3]
    assert next(it) == [4]
    with pytest.raises(StopIteration):
        next(it)


def test_chunked_iterator_all():
    data = [1, 2, 3, 4]
    it = ChunkedIterable(iter(data), 0)
    assert next(it) == [1, 2, 3, 4]
    with pytest.raises(StopIteration):
        next(it)
