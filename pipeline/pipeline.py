import abc
import logging

LOGGER = logging.getLogger(__name__)


class Task(abc.ABC):
    """A task in a pipeline

    Implementations must define a process() method.
    Any initialization or cleanup can be done in begin() or end().
    See Pipeline for how to construct a pipeline of tasks.
    """

    def __init__(self):
        self.downstream = None

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

    def _process(self, item):
        """Push the output of process() to the next task"""
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

    def __or__(self, other):
        # a chain of tasks gets handled from left to right.
        # we traverse rightward until we hit the first task that hasn't been connected.
        left = self
        while left.downstream:
            left = left.downstream
        left.downstream = other
        return self


class Pipeline:
    def __init__(self, task):
        self.task = task
        self.count = 0

    def run(self, iterable):
        self.begin()
        for item in iterable:
            self.task._process(item)
            self.count += 1
        self.end()

    def begin(self):
        self.count = 0
        self.task._begin()

    def end(self):
        self.task._end()

    def __str__(self):
        task_names = []
        task = self.task
        while task:
            task_names.append(str(task.__class__.__name__))
            task = task.downstream
        return ' | '.join(task_names)
