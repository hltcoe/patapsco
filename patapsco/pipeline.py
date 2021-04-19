import abc
import logging

from .util import Timer, TimedIterable

LOGGER = logging.getLogger(__name__)


class Task(abc.ABC):
    """A task in a pipeline

    Implementations must define a process() method.
    Any initialization or cleanup can be done in begin() or end().
    See Pipeline for how to construct a pipeline of tasks.
    """

    def __init__(self):
        self.downstream = None
        self.timer = Timer()

    @abc.abstractmethod
    def process(self, item):
        """Process an item

        A task must implement this method.
        It must return a new item that resulted from processing or the original item.
        """
        pass

    def begin(self):
        """Optional begin method for initialization"""
        pass

    def end(self):
        """Optional end method for cleaning up"""
        pass

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def time(self):
        return self.timer.time

    def _process(self, item):
        """Push the output of process() to the next task"""
        with self.timer:
            item = self.process(item)
        if self.downstream:
            self.downstream._process(item)

    def _begin(self):
        self.begin()
        if self.downstream:
            self.downstream._begin()

    def _end(self):
        self.end()
        if self.downstream:
            self.downstream._end()


class MultiplexItem:
    """Supports passing multiple items from Task.process"""
    def __init__(self):
        self.items = {}

    def add(self, name, item):
        self.items[name] = item


class Pipeline:
    def __init__(self, tasks, iterable):
        self.task = self._connect(tasks)
        self.iterable = TimedIterable(iterable)
        self.count = 0

    def run(self):
        self.begin()
        for item in self.iterable:
            self.task._process(item)
            self.count += 1
        self.end()

    def begin(self):
        self.count = 0
        self.task._begin()

    def end(self):
        self.task._end()

    @property
    def report(self):
        task = self.task
        report = [(self.iterable.name, self.iterable.time), (task.name, task.time)]
        while task.downstream:
            task = task.downstream
            report.append((task.name, task.time))
        return report

    def _connect(self, tasks):
        head_task = prev_task = tasks.pop(0)
        while tasks:
            cur_task = tasks.pop(0)
            prev_task.downstream = cur_task
            prev_task = cur_task
        return head_task

    def __str__(self):
        task_names = [self.iterable.name]
        task = self.task
        while task:
            task_names.append(task.name)
            task = task.downstream
        return ' | '.join(task_names)
