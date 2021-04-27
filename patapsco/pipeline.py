import abc
import json
import logging
import pathlib

from .config import ConfigService
from .util import Timer, TimedIterable, ChunkedIterable
from .util.file import touch_complete

LOGGER = logging.getLogger(__name__)


class Task(abc.ABC):
    """A task in a pipeline

    Implementations must define a process() method.
    Any initialization or cleanup can be done in begin() or end().
    See Pipeline for how to construct a pipeline of tasks.
    """

    @abc.abstractmethod
    def process(self, item):
        """Process an item

        A task must implement this method.
        It must return a new item that resulted from processing or the original item.
        """
        pass

    def batch_process(self, items):
        """Process a batch of items

        Args:
            items (list): A list of items

        Returns:
            list of items
        """
        return [self.process(item) for item in items]

    def begin(self):
        """Optional begin method for initialization"""
        pass

    def end(self):
        """Optional end method for cleaning up"""
        pass

    def __str__(self):
        return self.__class__.__name__


class TimedTask(Task):
    """Task with a built in timer that wraps another task"""
    def __init__(self, task):
        self.task = task
        self.timer = Timer()

    def process(self, item):
        with self.timer:
            return self.task.process(item)

    def batch_process(self, items):
        return self.task.batch_process(items)

    def begin(self):
        self.task.begin()

    def end(self):
        self.task.end()

    @property
    def time(self):
        return self.timer.time

    def __str__(self):
        return str(self.task)


class MultiplexItem:
    """Supports passing multiple items from Task.process"""
    def __init__(self):
        self._items = {}

    def add(self, name, item):
        self._items[name] = item

    def items(self):
        """
        Returns an iterable over key-value pairs
        """
        return self._items.items()


class MultiplexTask(Task):
    """Accepts a MultiplexItem and wraps the tasks for each item in it"""

    def __init__(self, splits, create_fn, config, artifact_config, *args, **kwargs):
        """
        Args:
            splits (list of str or dict of tasks): List of split identifiers or list of Tasks to be multiplexed.
            create_fn (callable): Function to create a task per split.
            config (BaseConfig): Config for the tasks.
            artifact_config (BaseConfig): Config that resulted in this artifact.
        """
        super().__init__()
        if isinstance(splits, dict):
            self.tasks = splits
        else:
            self.tasks = {}
            for split in splits:
                task_config = config.copy(deep=True)
                if task_config.output:
                    self.dir = pathlib.Path(config.output.path)
                    task_config.output.path = str(pathlib.Path(task_config.output.path) / split)
                self.tasks[split] = create_fn(task_config, artifact_config, *args, **kwargs)
            self.artifact_config = artifact_config
            self.config_path = self.dir / 'config.yml'
            # we save the splits for components downstream to access
            with open(self.dir / '.multiplex', 'w') as fp:
                json.dump(splits, fp)

    def process(self, item):
        new_item = MultiplexItem()
        for name, value in item.items():
            new_item.add(name, self.tasks[name].process(value))
        return new_item

    def begin(self):
        for task in self.tasks.values():
            task.begin()

    def end(self):
        for task in self.tasks.values():
            task.end()
        if hasattr(self, 'dir'):
            if self.artifact_config:
                ConfigService.write_config_file(self.config_path, self.artifact_config)
            touch_complete(self.dir)

    @property
    def name(self):
        return f"Multiplex({list(self.tasks.values())[0].name})"


class Pipeline(abc.ABC):
    """Interface for a pipeline of tasks"""

    def __init__(self, iterable, tasks):
        """
        Args:
            iterable (iterable): Iterable of input for pipeline.
            tasks (list): List of tasks run in sequence.
        """
        self.iterable = TimedIterable(iterable)
        self.tasks = [TimedTask(task) for task in tasks]
        self.count = 0

    @abc.abstractmethod
    def run(self):
        pass

    def begin(self):
        self.count = 0
        for task in self.tasks:
            task.begin()

    def end(self):
        for task in self.tasks:
            task.end()

    @property
    def report(self):
        report = [(str(self.iterable), self.iterable.time)]
        report.extend((str(task), task.time) for task in self.tasks)
        return report

    def __str__(self):
        task_names = [str(self.iterable)]
        task_names.extend(str(task) for task in self.tasks)
        return ' | '.join(task_names)


class StreamingPipeline(Pipeline):
    """Pipeline that streams one item at a time through the tasks"""

    def run(self):
        self.begin()
        for item in self.iterable:
            for task in self.tasks:
                item = task.process(item)
            self.count += 1
        self.end()


class BatchPipeline(Pipeline):
    """Pipeline that pushes chunks of input through the tasks"""

    def __init__(self, iterable, tasks, n):
        """
        Args:
            iterable (iterable): Iterator that generates input for the pipeline.
            tasks (list): List of tasks.
            n (int): Batch size or None to process all.
        """
        super().__init__(ChunkedIterable(iterable, n), tasks)

    def run(self):
        self.begin()
        for chunk in self.iterable:
            self.count += len(chunk)
            for task in self.tasks:
                chunk = task.batch_process(chunk)
        self.end()
