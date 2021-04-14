import dataclasses
import json
import sys

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
