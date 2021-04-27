import collections.abc
import dataclasses
import json
import sys
import timeit

import more_itertools

from ..config import BaseConfig
from ..error import ConfigError
from .file import GlobFileGenerator, validate_encoding


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
    """Same as ComponentFactory but wrapped in a GlobFileGenerator"""
    @classmethod
    def create(cls, config, *args, **kwargs):
        """
        Args:
            config (DocumentsInputConfig or TopicsInputConfig)
        """
        validate_encoding(config.encoding)
        # support passing additional args to reader constructors
        args = {key: value for key, value in config.dict().items() if key not in ['format', 'path', 'encoding', 'lang']}
        return GlobFileGenerator(config.path, cls._get_class(config), config.encoding, config.lang, **args)


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


class TimedIterable(collections.abc.Iterable):
    def __init__(self, iterable):
        self.iterable = iterable
        self.timer = Timer()

    @property
    def time(self):
        return self.timer.time

    def __str__(self):
        return self.iterable.__class__.__name__

    def __iter__(self):
        return self

    def __next__(self):
        with self.timer:
            return next(self.iterable)


class ChunkedIterable(collections.abc.Iterable):
    def __init__(self, iterable, n):
        self.iterable = iterable
        self.chunked = more_itertools.chunked(iterable, n)
        self.n = n
        self.done = False

    def __str__(self):
        return self.iterable.__class__.__name__

    def __iter__(self):
        return self

    def __next__(self):
        if self.n == 0:
            if not self.done:
                self.done = True
                return [x for x in self.iterable]
            else:
                raise StopIteration()
        else:
            return next(self.chunked)
