import json
import logging
import pathlib
import re
from typing import Optional, Union

import pydantic
import yaml

from .error import ConfigError

LOGGER = logging.getLogger(__name__)


class BaseConfig(pydantic.BaseModel):
    """Base class of configuration objects

    This uses dataclasses and includes validation of parameters.
    """
    class Config:
        extra = pydantic.Extra.forbid


def load_config(filename):
    """Loads the configuration detecting file type

    Args:
        filename (str): path to the configuration file

    Returns:
        dict
    """
    ext = pathlib.Path(filename).suffix.lower()
    if ext in ['.yaml', '.yml']:
        reader_fn = load_yaml_config
    elif ext == '.json':
        reader_fn = load_json_config
    else:
        raise ConfigError(f"Unknown config file extension {ext}")
    with open(filename, 'r') as fp:
        LOGGER.info("Loading configuration from %s", filename)
        return reader_fn(fp)


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
    interpolator = ConfigInterpolator()
    conf = convert_dict(conf)
    interpolator.interpolate(conf, conf)
    conf = unconvert_dict(conf)
    if interpolator.errors:
        error_string = ', '.join(interpolator.errors)
        raise ConfigError(f"Missing interpolations in config: {error_string}")
    return conf


def save_json_config(file, data):
    """Save a configuration to a JSON file

    Args:
        file (file): file object opened for writing
        data (dict): data to write as YAML
    """
    json.dump(data, file, indent=4, sort_keys=True)


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


def unconvert_dict(d):
    """Convert attribute dictionary back to a normal dictionary

    Handles nested dictionaries and lists.

    Args:
        d (AttrDict)

    Returns:
        dict
    """
    d = dict(d)
    for key, value in d.items():
        if isinstance(value, AttrDict):
            d[key] = unconvert_dict(value)
        elif isinstance(value, list):
            for index, entry in enumerate(value):
                if isinstance(entry, AttrDict):
                    value[index] = unconvert_dict(entry)
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
        data = convert_dict(data)
        self.interpolator.interpolate(data, data)
        data = unconvert_dict(data)
        self.errors = self.interpolator.errors
        return data


class ConfigInterpolator:
    def __init__(self):
        self.regx = re.compile('.*{.*}.*')
        self.errors = []

    def interpolate(self, data, mapping):
        for key, value in data.items():
            data[key] = self.interpolate_value(value, mapping)

    def interpolate_value(self, value, mapping):
        if isinstance(value, str) and self.regx.match(value) is not None:
            try:
                value = value.format_map(mapping)
            except (AttributeError, KeyError):
                self.errors.append(value)
        elif isinstance(value, list):
            value = [self.interpolate_value(entry, mapping) for entry in value]
        elif isinstance(value, dict):
            self.interpolate(value, mapping)
        return value


class ConfigOverrides:
    """Overrides values in a configuration dictionary

    Nested values are specified with dot delimiters: rerank.embedding.length
    This does not currently support lists.
    All values are set as strings except for booleans.
    The key must already exist in the configuration dictionary.
    """
    @classmethod
    def process(cls, config, overrides):
        """Process a list of configuration overrides

        Args:
            config (dict): Configuration dictionary loaded from yaml or json file.
            overrides (list): List of strings of form: key=value
        """
        if overrides:
            overrides = [override.split('=') for override in overrides]
            cls._update_booleans(overrides)
            for k, v in overrides:
                if '.' in k:
                    keys = k.split('.')
                    last_key = keys.pop()
                    d = config
                    for key in keys:
                        try:
                            d = d[key]
                        except KeyError:
                            raise ConfigError(f"Unknown override parameter {k}")
                    if last_key not in d:
                        raise ConfigError(f"Unknown override parameter {k}")
                    d[last_key] = v
                else:
                    if k not in config:
                        raise ConfigError(f"Unknown override parameter {k}")
                    config[k] = v

    @staticmethod
    def _update_booleans(overrides):
        for override in overrides:
            if override[1] in ['true', 'on', 'yes']:
                override[1] = True
            elif override[1] in ['false', 'off', 'no']:
                override[1] = False
