import abc
import logging
import pathlib

from .config import ConfigService
from .util import Timer, TimedIterator, ChunkedIterator
from .util.file import touch_complete

LOGGER = logging.getLogger(__name__)


class Task(abc.ABC):
    """A task in a pipeline

    Implementations must define a process() method.
    Any initialization or cleanup can be done in begin() or end().
    See Pipeline for how to construct a pipeline of tasks.
    """

    def __init__(self, run_path=None, artifact_config=None, base=None):
        """
        Args:
            run_path (str): Root directory of the run.
            artifact_config (BaseConfig): Config for all tasks up to this task.
            base (Path): Relative path to directory of task from run root.
        """
        self.artifact_config = artifact_config
        self.run_path = pathlib.Path(run_path) if run_path else None
        if base is not None:
            base = self.run_path / base
            base.mkdir(parents=True, exist_ok=True)
        self.base = base

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
        """End method for cleaning up and marking as complete"""
        if self.base:
            ConfigService.write_config_file(self.base / 'config.yml', self.artifact_config)
            touch_complete(self.base)

    def reduce(self, dirs):
        """Reduce output across parallel jobs

        Args:
            dirs (list): List of directories with partial output
        """
        pass

    def run_reduce(self):
        """Method for pipeline to call to run reduce()"""
        if self.base:
            dirs = sorted(list(self.base.glob('part*')))
            self.reduce(dirs)

    def __str__(self):
        return self.__class__.__name__


class TimedTask(Task):
    """Task with a built in timer that wraps another task"""
    def __init__(self, task):
        super().__init__()
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

    def reduce(self, dirs):
        self.task.reduce(dirs)

    def run_reduce(self):
        self.task.run_reduce()

    @property
    def time(self):
        return self.timer.time

    def __str__(self):
        return str(self.task)


class Pipeline(abc.ABC):
    """Interface for a pipeline of tasks"""

    def __init__(self, iterator, tasks):
        """
        Args:
            iterator (iterator): Iterator over input for pipeline.
            tasks (list): List of tasks run in sequence.
        """
        self.iterator = TimedIterator(iterator)
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

    def reduce(self):
        for task in self.tasks:
            task.run_reduce()

    @property
    def report(self):
        report = [(str(self.iterator), self.iterator.time)]
        report.extend((str(task), task.time) for task in self.tasks)
        return report

    def __str__(self):
        task_names = [str(self.iterator)]
        task_names.extend(str(task) for task in self.tasks)
        return ' | '.join(task_names)


class StreamingPipeline(Pipeline):
    """Pipeline that streams one item at a time through the tasks"""

    def run(self):
        self.begin()
        for item in self.iterator:
            for task in self.tasks:
                item = task.process(item)
            self.count += 1
        self.end()


class BatchPipeline(Pipeline):
    """Pipeline that pushes chunks of input through the tasks"""

    def __init__(self, iterator, tasks, n):
        """
        Args:
            iterator (iterator): Iterator that produces input for the pipeline.
            tasks (list): List of tasks.
            n (int): Batch size or None to process all.
        """
        super().__init__(ChunkedIterator(iterator, n), tasks)

    def run(self):
        self.begin()
        for chunk in self.iterator:
            self.count += len(chunk)
            for task in self.tasks:
                chunk = task.batch_process(chunk)
        self.end()
