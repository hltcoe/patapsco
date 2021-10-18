import abc
import collections.abc
import contextlib
import dataclasses
import glob
import itertools
import json
import logging
import sys
import timeit

import more_itertools
import pycountry

from ..error import BadDataError, ConfigError
from .file import validate_encoding


def get_logger(name):
    return logging.getLogger(f"patapsco.{name}")


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
    def register(cls, name, class_obj):
        """Register a class object with the foctory

        Args:
            name (str): nickname of the class used in configuration
            class_obj (class): class object used to construct the component

        Returns:
            class object
        """
        if isinstance(class_obj, str):
            raise ConfigError(f"The class object for {name} must class and not a string")
        cls.classes[name] = class_obj

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
            # if user registered a class instance, we return that. Otherwise, look up the class name in local namespace.
            if isinstance(class_name, str):
                return namespace[class_name]
            else:
                return class_name
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
        reader_cls = cls._get_class(config)
        # support passing additional args to reader constructors
        args = {key: value for key, value in config.dict().items() if key not in ['format', 'path', 'encoding', 'lang']}
        if issubclass(reader_cls, NoGlobSupport):
            return reader_cls(config.path, config.encoding, config.lang, **args)
        return GlobIterator(config.path, reader_cls, config.encoding, config.lang, **args)


class TaskFactory(ComponentFactory):
    """Same as ComponentFactory includes the run path"""
    @classmethod
    def create(cls, run_path, config, *args, **kwargs):
        """
        Args:
            run_path (str): Root path of the run.
            config (DocumentsInputConfig or TopicsInputConfig)
        """
        return cls._get_class(config)(run_path, config, *args, **kwargs)


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

    def __str__(self):
        return self.__class__.__name__


class TimedIterator(collections.abc.Iterator):
    def __init__(self, iterator):
        self.iterator = iterator
        self.timer = Timer()

    @property
    def time(self):
        return self.timer.time

    def __str__(self):
        return str(self.iterator)

    def __next__(self):
        with self.timer:
            return next(self.iterator)

    def __len__(self):
        return len(self.iterator)


class ChunkedIterator(InputIterator):
    """Iterate over iterable in chunks of size n"""

    def __init__(self, iterable, n):
        """
        Args:
            iterable (iterable)
            n (int): chunk size or None to consume the entire iterable in a single chunk
        """
        self.iterable = iterable
        self.chunked = more_itertools.chunked(iterable, n)
        self.n = n
        self.done = False

    def __str__(self):
        return self.iterable.__class__.__name__

    def __next__(self):
        if not self.n:  # single chunk
            if not self.done:
                self.done = True
                return [x for x in self.iterable]
            else:
                raise StopIteration()
        else:
            return next(self.chunked)

    def __len__(self):
        return len(self.iterable)


class SlicedIterator(InputIterator):
    """Support start and stop offsets on InputIterator"""

    def __init__(self, iterator, start, stop):
        self.original_iterator = iterator
        self.start = start
        self.stop = stop
        if start is None and stop is None:
            self.iterator = iterator
        elif start is not None and hasattr(iterator, "skip"):
            iterator.skip(start)
            if stop:
                self.iterator = itertools.islice(iterator, stop - start)
            else:
                self.iterator = iterator
        else:
            self.iterator = itertools.islice(iterator, start, stop)

    def __next__(self):
        return next(self.iterator)

    def __len__(self):
        original_length = len(self.original_iterator)
        start = self.start if self.start else 0
        if not self.stop:
            return original_length - start
        else:
            return min(original_length, self.stop) - start

    def __str__(self):
        return str(self.original_iterator)


class NoGlobSupport:
    """Indicate that this iterator does not support the GlobIterator"""
    pass


class GlobIterator(InputIterator):
    """
    You have a callable that returns an iterator over items given a file.
    You have one or more globs that match files.
    You want to seamlessly iterator over the callable across the files that match.
    Use GlobIterator.
    """

    def __init__(self, globs, cls, *args, **kwargs):
        """
        Args:
            globs (list or str): array of glob strings or single glob string
            cls (class): InputIterator class
            *args: variable length arguments for the parsing function
            **kwargs: keyword arguments for the parsing function
        """
        if isinstance(globs, str):
            globs = [globs]
        self.original_globs = globs
        self.globs = iter(globs)
        self.cls = cls
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
                # exhausted the files that match the previous glob, so advance to the next one
                self.paths = iter(self._next_glob())
                self.gen = self._next_generator()
            return self.__next__()

    def __len__(self):
        count = 0
        for pattern in self.original_globs:
            for path in glob.glob(pattern):
                reader = self.cls(path, *self.args, **self.kwargs)
                count += len(reader)
        return count

    def __str__(self):
        return str(self.cls.__name__)

    def skip(self, start):
        # TODO replace the skip to starting position with something more efficient
        if start:
            for _ in range(start):
                next(self)

    def _next_glob(self):
        self.pattern = next(self.globs)
        return sorted(glob.glob(self.pattern))

    def _next_generator(self):
        path = next(self.paths)
        self.first_use_of_gen = True
        return self.cls(path, *self.args, **self.kwargs)

    @staticmethod
    def _validate_globs(globs):
        for pattern in globs:
            if not glob.glob(pattern):
                raise ConfigError(f"No files match pattern '{pattern}'")


class LoggingFilter(logging.Filter):
    """Preprocess some logging messages"""

    def filter(self, record):
        # stanza has some annoying logging that we clean up
        if record.name == 'stanza':
            # stanza can pass a dictionary or tuple as the message sometimes
            if isinstance(record.msg, dict) or isinstance(record.msg, tuple):
                record.msg = str(record.msg)
            if record.msg.startswith('Loading these models for language'):
                lines = record.msg.split('\n')
                lines = [line for line in lines if "====" not in line and "----" not in line and line]
                lines.pop(1)  # remove table heading
                msg = lines.pop(0)
                record.msg = f"{msg} {', '.join(lines)}"
        return True


class LangStandardizer:
    """Utility class for language codes"""

    @classmethod
    def language(cls, code):
        lang = None
        if len(code) == 2:
            lang = pycountry.languages.get(alpha_2=code)
        elif len(code) == 3:
            lang = pycountry.languages.get(alpha_3=code)
        if lang is None:
            raise ConfigError(f"Unknown language code: {code}")
        return lang

    @classmethod
    def iso_639_3(cls, code):
        """
q        Args:
            code (str): 2 or 3 letter code

        Returns:
            str: ISO 639-3 language code
        """
        return cls.language(code).alpha_3

    @classmethod
    def iso_639_1(cls, code):
        """
        Args:
            code (str): 2 or 3 letter code

        Returns:
            str: ISO 639-1 language code
        """
        return cls.language(code).alpha_2


def get_human_readable_size(size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0 or unit == 'TB':
            break
        size /= 1024.0
    return f"{size:.1f} {unit}"


@contextlib.contextmanager
def ignore_exception(exception):
    try:
        yield
    except exception:
        pass
