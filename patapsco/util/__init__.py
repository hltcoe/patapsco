import abc
import collections.abc
import dataclasses
import glob
import itertools
import json
import sys
import timeit

import more_itertools

from ..config import BaseConfig
from ..error import BadDataError, ConfigError
from .file import validate_encoding


class ComponentFactory:
    classes = {}
    config_class = None
    name = "component"

    @classmethod
    def create(cls, config, *args, **kwargs):
        """
        Args:
            config (BaseConfig)
        """
        return cls._get_class(config)(config, *args, **kwargs)

    @classmethod
    def _get_class(cls, config):
        """
        Args:
            config (dict, BaseConfig)
        """
        namespace = vars(sys.modules[cls.__module__])
        if hasattr(config, 'name'):
            component_type = config.name
        elif hasattr(config, 'format'):
            component_type = config.format
        else:
            raise RuntimeError("Component has no name or format")
        try:
            class_name = cls.classes[component_type]
        except KeyError:
            raise ConfigError(f"Unknown {cls.name}: {component_type}")
        try:
            return namespace[class_name]
        except KeyError:
            raise RuntimeError(f"Cannot find {class_name} in {cls.__name__}")


class ReaderFactory(ComponentFactory):
    """Same as ComponentFactory but wrapped in a GlobIterator"""
    @classmethod
    def create(cls, config, *args, **kwargs):
        """
        Args:
            config (DocumentsInputConfig or TopicsInputConfig)
        """
        validate_encoding(config.encoding)
        # support passing additional args to reader constructors
        args = {key: value for key, value in config.dict().items() if key not in ['format', 'path', 'encoding', 'lang']}
        return GlobIterator(config.path, cls._get_class(config), config.encoding, config.lang, **args)


class DataclassJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if dataclasses.is_dataclass(obj):
            return dataclasses.asdict(obj)
        return super().default(obj)


class Timer:
    def __init__(self, name=None):
        self.name = name
        self.time = 0

    def __enter__(self):
        self.start = timeit.default_timer()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.time += timeit.default_timer() - self.start


class InputIterator(abc.ABC, collections.abc.Iterator, collections.abc.Sized):
    """Iterable that also supports len()"""
    pass


class TimedIterator(collections.abc.Iterator):
    def __init__(self, iterable):
        self.iterable = iterable
        self.timer = Timer()

    @property
    def time(self):
        return self.timer.time

    def __str__(self):
        return self.iterable.__class__.__name__

    def __next__(self):
        with self.timer:
            return next(self.iterable)


class ChunkedIterator(collections.abc.Iterator):
    def __init__(self, iterable, n):
        self.iterable = iterable
        self.chunked = more_itertools.chunked(iterable, n)
        self.n = n
        self.done = False

    def __str__(self):
        return self.iterable.__class__.__name__

    def __next__(self):
        if self.n == 0:
            if not self.done:
                self.done = True
                return [x for x in self.iterable]
            else:
                raise StopIteration()
        else:
            return next(self.chunked)


class GlobIterator(InputIterator):
    """
    You have a callable that returns an iterator over items given a file.
    You have one or more globs that match files.
    You want to seamlessly iterator over the callable across the files that match.
    Use GlobIterator.
    """

    def __init__(self, globs, func, *args, **kwargs):
        """
        Args:
            globs (list or str): array of glob strings or single glob string
            func (callable): parsing function returns an iterator
            *args: variable length arguments for the parsing function
            **kwargs: keyword arguments for the parsing function
        """
        if isinstance(globs, str):
            globs = [globs]
        self.original_globs = globs
        self.globs = iter(globs)
        self.parsing_func = func
        self.args = args
        self.kwargs = kwargs

        self._validate_globs(self.original_globs)

        self.pattern = None
        self.first_use_of_gen = True
        paths = self._next_glob()
        self.paths = iter(paths)
        self.gen = self._next_generator()

    def __next__(self):
        try:
            item = next(self.gen)
            self.first_use_of_gen = False
            return item
        except StopIteration:
            if self.first_use_of_gen:
                # bad file so we throw an exception
                raise BadDataError(f"{self.pattern} did not result in any items")
            try:
                self.gen = self._next_generator()
            except StopIteration:
                self.paths = iter(self._next_glob())
            return self.__next__()

    def __len__(self):
        count = 0
        for pattern in self.original_globs:
            for path in glob.glob(pattern):
                reader = self.parsing_func(path, *self.args, **self.kwargs)
                count += len(reader)
        return count

    def slice(self, start, stop):
        # TODO replace the skip to starting position with something more efficient
        if start and stop:
            stop -= start
        if start:
            for _ in range(start):
                next(self)
        return itertools.islice(self, stop)

    def _next_glob(self):
        self.pattern = next(self.globs)
        return sorted(glob.glob(self.pattern))

    def _next_generator(self):
        path = next(self.paths)
        self.first_use_of_gen = True
        return self.parsing_func(path, *self.args, **self.kwargs)

    @staticmethod
    def _validate_globs(globs):
        for pattern in globs:
            if not glob.glob(pattern):
                raise ConfigError(f"No files match pattern '{pattern}'")
