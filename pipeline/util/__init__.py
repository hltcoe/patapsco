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
        try:
            class_name = cls.classes[config.name]
        except KeyError:
            raise ConfigError(f"Unknown component: {config.name}")
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
