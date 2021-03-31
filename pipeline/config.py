import json
import re
from typing import Optional

import pydantic
import yaml

from .error import *


class BaseConfig(pydantic.BaseModel):
    """Base class of configuration objects

    This uses dataclasses and includes validation of parameters.
    """
    class Config:
        extra = pydantic.Extra.forbid


def load_yaml_config(stream):
    """Loads a configuration from a YAML stream

    Args:
        stream (file, str): File like object or string to parse

    Returns:
        dict
    """
    loader = ConfigLoader(stream)
    conf = yaml.load(stream, Loader=loader)
    if loader.errors:
        error_string = ', '.join(loader.errors)
        raise ConfigError(f"Missing interpolations in config: {error_string}")
    return conf


def save_yaml_config(file, data):
    """Save a configuration to a YAML file

    Args:
        file (file): file object opened for writing
        data (dict): data to write as YAML
    """
    yaml.dump(data, file)


def load_json_config(file):
    """Loads a configuration from a JSON file

    Args:
        file (file): File like object to parse

    Returns:
        dict
    """
    conf = json.load(file)
    return conf


class AttrDict(dict):
    """Dictionary that supports access of values as attributes"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self


def convert_dict(d):
    """Convert normal dictionary to an attribute dictionary

    Handles nested dictionaries and lists.

    Args:
        d (dict): A python dictionary

    Returns:
        AttrDict
    """
    d = AttrDict(d)
    for key, value in d.items():
        if isinstance(value, dict):
            d[key] = convert_dict(value)
        elif isinstance(value, list):
            for index, entry in enumerate(value):
                if isinstance(entry, dict):
                    value[index] = convert_dict(entry)
    return d


class ConfigLoader(yaml.FullLoader):
    """YAML loader that handles interpolation

    Example:
        This loader handles interpolation of configuration values like this:
          lang: zh
          output: "output/{lang}.txt"
        Nested values are specified with dot separators: retrieve.component.param
    """
    def __init__(self, stream):
        super().__init__(stream)
        self.errors = []
        self.interpolator = ConfigInterpolator()

    def __call__(self, stream):
        # yaml.load wants a class object to constructor, but we want to maintain errors
        return self

    def get_single_data(self):
        data = super().get_single_data()
        data = self.interpolator.interpolate(data, convert_dict(data))
        self.errors = self.interpolator.errors
        return data


class ConfigInterpolator:
    def __init__(self):
        self.regx = re.compile('.*{.*}.*')
        self.errors = []

    def interpolate(self, data, mapping):
        for key, value in data.items():
            data[key] = self.interpolate_value(value, mapping)
        return data

    def interpolate_value(self, value, mapping):
        if isinstance(value, str) and self.regx.match(value) is not None:
            try:
                value = value.format_map(mapping)
            except AttributeError:
                self.errors.append(value)
        elif isinstance(value, list):
            value = [self.interpolate_value(entry, mapping) for entry in value]
        elif isinstance(value, dict):
            value = self.interpolate(value, mapping)
        return value
