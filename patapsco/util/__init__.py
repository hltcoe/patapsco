import collections
import dataclasses
import json
import sys
import timeit

from ..config import BaseConfig
from ..error import ConfigError


class ComponentFactory:
    classes = {}
    config_class = None

    @classmethod
    def create(cls, config, *args, **kwargs):
        """
        Args:
            config (dict, BaseConfig)
        """
        if not isinstance(config, BaseConfig):
            config = cls.config_class(**config)
        namespace = vars(sys.modules[cls.__module__])
        if hasattr(config, 'name'):
            name = config.name
        elif hasattr(config, 'format'):
            name = config.format
        else:
            raise RuntimeError(f"Component has no name or format")
        try:
            class_name = cls.classes[name]
        except KeyError:
            raise ConfigError(f"Unknown component: {name}")
        try:
            class_ = namespace[class_name]
        except KeyError:
            raise RuntimeError(f"Cannot find {class_name} in {cls.__name__}")
        return class_(config, *args, **kwargs)


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


class TimedIterable(collections.Iterable):
    def __init__(self, iterable):
        self.iterable = iterable
        self.timer = Timer()

    @property
    def time(self):
        return self.timer.time

    @property
    def name(self):
        return self.iterable.__class__.__name__

    def __iter__(self):
        return self

    def __next__(self):
        with self.timer:
            return next(self.iterable)
