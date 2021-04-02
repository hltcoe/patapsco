import copy
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
    update_booleans(conf)
    return conf


def update_booleans(d):
    for key, value in d.items():
        if isinstance(value, dict):
            update_booleans(value)
        elif isinstance(value, list):
            for index, entry in enumerate(value):
                if isinstance(entry, dict):
                    update_booleans(entry)
        elif isinstance(value, str):
            if value in ['true', 'on', 'yes']:
                d[key] = True
            elif value in ['false', 'off', 'no']:
                d[key] = False


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


class FlatDict:
    """Nested dictionary wrapper that works with keys of the form x.y.z"""
    def __init__(self, d, add_keys=False):
        """
        Args:
            d (dict): dictionary to wrap
            add_keys (bool): whether to create new keys with set
        """
        self.d = d
        self.add_keys = add_keys

    def __getitem__(self, key):
        if '.' in key:
            keys = key.split('.')
            last_key = keys.pop()
            d = self.d
            for k in keys:
                d = d[k]
            return d[last_key]
        else:
            return self.d[key]

    def __setitem__(self, key, value):
        if '.' in key:
            keys = key.split('.')
            last_key = keys.pop()
            d = self.d
            for k in keys:
                if not self.add_keys and k not in d:
                    raise KeyError(key)
                d = d[k]
            if not self.add_keys and last_key not in d:
                raise KeyError(key)
            d[last_key] = value
        else:
            if not self.add_keys and key not in self.d:
                raise KeyError(key)
            self.d[key] = value


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
            d = FlatDict(config)
            for k, v in overrides:
                try:
                    d[k] = v
                except KeyError:
                    raise ConfigError(f"Unknown override parameter {k}")

    @staticmethod
    def _update_booleans(overrides):
        for override in overrides:
            if override[1] in ['true', 'on', 'yes']:
                override[1] = True
            elif override[1] in ['false', 'off', 'no']:
                override[1] = False


class ConfigInheritance:
    """Configuration sections can inherit from other sections

    This is enabled using the key 'inherit' with the value as the section inherited from.
    The keys in the child config will overrides those in the parent.
    """
    @classmethod
    def process(cls, config, top_config):
        """Process a configuration for inheritance

        Args:
            config (dict): Configuration dictionary being processed.
            top_config (dict): Top level config loaded from yaml or json.
        """
        for key, value in config.items():
            if isinstance(value, dict):
                cls.process(value, top_config)
                if 'inherit' in value:
                    try:
                        parent = cls._get_parent(value['inherit'], top_config)
                    except KeyError:
                        raise ConfigError(f"Cannot inherit from {value['inherit']} as it does not exist")
                    new_conf = copy.deepcopy(parent)
                    cls._merge(new_conf, config[key])
                    config[key] = new_conf
                    del config[key]['inherit']
            elif isinstance(value, list):
                for index, entry in enumerate(value):
                    if isinstance(entry, dict):
                        cls.process(entry, top_config)

    @staticmethod
    def _get_parent(parent_key, top_config):
        d = FlatDict(top_config)
        return d[parent_key]

    @classmethod
    def _merge(cls, d1, d2):
        # if d1[k] is not a dict and d2[k] is, d1[k] is overwritten with d2[k]
        # if d1[k] is a dict and d2[k] is not, d1[k] is overwritten with d2[k]
        for k, v in d2.items():
            if k in d1 and isinstance(d1[k], dict) and isinstance(d2[k], dict):
                cls._merge(d1[k], d2[k])
            else:
                d1[k] = d2[k]
