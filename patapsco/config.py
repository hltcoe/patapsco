import copy
import enum
import json
import logging
import pathlib
import re
from typing import Optional, Union

import pydantic
import yaml

from .error import ConfigError

LOGGER = logging.getLogger(__name__)

# pydantic is removing "fields" as an attribute in 1.8.0 but still has a check for it
delattr(pydantic.BaseModel, 'fields')


class BaseConfig(pydantic.BaseModel):
    """Base class of configuration objects

    This uses dataclasses and includes validation of parameters.
    """

    def get(self, item):
        return getattr(self, item)

    def set(self, item, value):
        setattr(self, item, value)

    class Config:
        extra = pydantic.Extra.forbid


class SectionConfig(BaseConfig):
    """Top level section"""
    comment: Optional[str]


class UncheckedSectionConfig(BaseConfig):
    """Top level section that supports arbitrary parameters"""
    comment: Optional[str]

    class Config:
        extra = pydantic.Extra.allow


class ConfigService:
    """Configuration service

    Handles reading and writing configuration files.
    Supports:
      * String interpolation in values
      * Overriding configuration values
      * Section inheritance
    """

    YAML = 1
    JSON = 2

    def __init__(self, overrides=None, inheritance=True):
        """
        Args:
            overrides (list): A list of strings of the form key=value
            inheritance (bool): Whether to turn on inheritance support
        """
        self.overrides = overrides
        self.inheritance = inheritance

    @staticmethod
    def create_config_object(config_class, config_dict):
        """Create a configuration object using pydantic

        Performs validation

        Args:
            config_class (class): BaseConfig object to create
            config_dict (dict): Dictionary to convert to object
        """
        try:
            return config_class(**config_dict)
        except pydantic.ValidationError as error:
            # provides a more tailored error message than the default
            errors = {'.'.join(e['loc']): e['type'] for e in error.errors()}
            error_msg = f"{len(errors)} validation errors in configuration"
            for field, error_type in errors.items():
                if error_type == 'value_error.missing':
                    error_msg += f"\n  {field} - missing field"
                elif error_type == 'value_error.extra':
                    error_msg += f"\n  {field} - unknown field"
                elif error_type.startswith('type_error'):
                    error_msg += f"\n  {field} - wrong type, should be {error_type.split('.')[1]}"
                else:
                    error_msg += f"\n  {field} - {error_type}"
            raise ConfigError(error_msg)

    def read_config_file(self, filename):
        """Read the configuration detecting file type

        Args:
            filename (str or Path): path to the configuration file

        Returns:
            dict
        """
        ftype = self._detect_filetype(filename)
        if ftype == self.YAML:
            reader_fn = self._read_yaml_config
        else:
            reader_fn = self._read_json_config
        with open(filename, 'r') as fp:
            LOGGER.debug("Loading configuration from %s", filename)
            conf = reader_fn(fp)
            if 'imports' in conf:
                self._import_configs(conf, filename)
            if self.overrides:
                ConfigOverrides.process(conf, self.overrides)
            if self.inheritance:
                ConfigInheritance.process(conf)
            return conf

    @classmethod
    def write_config_file(cls, filename, config):
        """Write the configuration file detecting file type

        Args:
            filename (str or Path): path to the configuration file to write
            config (BaseConfig): configuration object
        """
        ftype = cls._detect_filetype(filename)
        if ftype == cls.YAML:
            writer_fn = cls._write_yaml_config
        else:
            writer_fn = cls._write_json_config
        with open(filename, 'w') as fp:
            writer_fn(fp, config.dict(exclude_none=True))

    @staticmethod
    def _detect_filetype(filename):
        """Detect file type from the filename"""
        ext = pathlib.Path(filename).suffix.lower()
        if ext in ['.yaml', '.yml']:
            return ConfigService.YAML
        elif ext == '.json':
            return ConfigService.JSON
        else:
            raise ConfigError(f"Unknown config file extension {ext} for {filename}")

    @staticmethod
    def _read_yaml_config(fp):
        """Read a configuration from a YAML stream

        Args:
            fp (file): File like object or string to parse

        Returns:
            dict
        """
        loader = ConfigLoader(fp)
        conf = yaml.load(fp, Loader=loader)
        if loader.errors:
            error_string = ', '.join(loader.errors)
            raise ConfigError(f"Missing interpolations in config: {error_string}")
        return conf

    @staticmethod
    def _write_yaml_config(fp, data):
        """Write a configuration to a YAML file

        Args:
            fp (file): file object opened for writing
            data (dict): data to write as YAML
        """
        yaml.dump(data, fp, Dumper=EnumDumper)

    def _read_json_config(self, fp):
        """Read a configuration from a JSON file

        Args:
            fp (file): File like object to parse

        Returns:
            dict
        """
        conf = json.load(fp)
        interpolator = ConfigInterpolator()
        conf = interpolator.interpolate(conf)
        if interpolator.errors:
            error_string = ', '.join(interpolator.errors)
            raise ConfigError(f"Missing interpolations in config: {error_string}")
        self._convert_boolean_strings(conf)
        return conf

    @classmethod
    def _convert_boolean_strings(cls, d):
        """Converts boolean-like strings to booleans in place.

        This makes the json reading compatible with the yaml reading.
        """
        for key, value in d.items():
            if isinstance(value, dict):
                cls._convert_boolean_strings(value)
            elif isinstance(value, list):
                for index, entry in enumerate(value):
                    if isinstance(entry, dict):
                        cls._convert_boolean_strings(entry)
            elif isinstance(value, str):
                if value in ['true', 'on', 'yes']:
                    d[key] = True
                elif value in ['false', 'off', 'no']:
                    d[key] = False

    @staticmethod
    def _write_json_config(file, data):
        """Write a configuration to a JSON file

        Args:
            file (file): file object opened for writing
            data (dict): data to write as YAML
        """
        json.dump(data, file, indent=4, sort_keys=True)

    def _import_configs(self, conf, filename):
        """Load the configs to import and merge into main conf"""
        base_dir = pathlib.Path(filename).parent
        imports = conf['imports']
        del conf['imports']
        for file in imports:
            filename = base_dir / file
            partial_conf = self.read_config_file(filename)
            merge_dicts(conf, partial_conf)
            # handle imports that have imports
            if 'imports' in conf:
                self._import_configs(conf, filename)


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
        data = self.interpolator.interpolate(data)
        self.errors = self.interpolator.errors
        return data


class EnumDumper(yaml.SafeDumper):
    """Dump enums using their value"""

    def represent_data(self, data):
        if isinstance(data, enum.Enum):
            data = data.value
        return super().represent_data(data)


class ConfigInterpolator:
    """Perform string interpolator on a config dictionary

    Values with {key} are updated.
    Nested keys are delimited with dots: first.second.third.
    The config dictionary is processed from first key to last, depth first.
    Values that depend on other interpolated values must be ordered top to bottom.
    """

    def __init__(self):
        self.regx = re.compile('.*{.*}.*')
        self.errors = []

    def interpolate(self, conf):
        """Perform string interpolator on dictionary

        Args:
            conf (dict): Configuration dictionary

        Returns:
            dict: Updated configuration dictionary
        """
        conf = convert_dict(conf)
        self._interpolate(conf, conf)
        return unconvert_dict(conf)

    def _interpolate(self, conf, mapping):
        for key, value in conf.items():
            conf[key] = self.interpolate_value(value, mapping)

    def interpolate_value(self, value, mapping):
        if isinstance(value, str) and self.regx.match(value) is not None:
            try:
                value = value.format_map(mapping)
            except (AttributeError, KeyError):
                self.errors.append(value)
        elif isinstance(value, list):
            value = [self.interpolate_value(entry, mapping) for entry in value]
        elif isinstance(value, dict):
            self._interpolate(value, mapping)
        return value


class ConfigOverrides:
    """Overrides values in a configuration dictionary

    Nested values are specified with dot delimiters: rerank.embedding.length
    This does not currently support lists.
    All values are set as strings except for booleans.
    The key must already exist in the configuration dictionary.
    """
    @classmethod
    def process(cls, conf, overrides):
        """Process a list of configuration overrides

        Args:
            conf (dict): Configuration dictionary loaded from yaml or json file.
            overrides (list): List of strings of form: key=value
        """
        if overrides:
            overrides = [override.split('=') for override in overrides]
            cls._update_booleans(overrides)
            d = FlatDict(conf)
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
    The keys in the child config will override those in the parent or be combined if dictionaries.
    Lists will not be combined, but replaced.
    Multiple inheritance is not supported.
    If using multiple levels of inheritance (grandparents), the parents must be defined before the children.
    """
    @classmethod
    def process(cls, conf):
        """Process a configuration for inheritance

        Args:
            conf (dict): Configuration dictionary being processed.
        """
        return cls._process(conf, conf)

    @classmethod
    def _process(cls, config, top_config):
        """Internal process method

        Args:
            config (dict): Current configuration dictionary being updated.
            top_config (dict): Top level config dictionary to find parents in.
        """
        for key, value in config.items():
            if isinstance(value, dict):
                cls._process(value, top_config)
                if 'inherit' in value:
                    try:
                        parent = cls._get_parent(value['inherit'], top_config)
                    except KeyError:
                        raise ConfigError(f"Cannot inherit from {value['inherit']} as it does not exist")
                    new_conf = copy.deepcopy(parent)
                    merge_dicts(new_conf, config[key])
                    config[key] = new_conf
                    del config[key]['inherit']
            elif isinstance(value, list):
                for index, entry in enumerate(value):
                    if isinstance(entry, dict):
                        cls._process(entry, top_config)

    @staticmethod
    def _get_parent(parent_key, top_config):
        d = FlatDict(top_config)
        return d[parent_key]


def merge_dicts(d1, d2):
    # if d1[k] is not a dict and d2[k] is, d1[k] is overwritten with d2[k]
    # if d1[k] is a dict and d2[k] is not, d1[k] is overwritten with d2[k]
    for k, v in d2.items():
        if k in d1 and isinstance(d1[k], dict) and isinstance(d2[k], dict):
            merge_dicts(d1[k], d2[k])
        else:
            d1[k] = d2[k]
