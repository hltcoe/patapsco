import logging

LOGGER = logging.getLogger(__name__)


class Module:
    """Pipeline module

    A module is an iterator that takes its input from an input iterator.
    A module should define an initializer and a process method.
    The process method is passed input from the previous module in the pipeline.
    The output of the process method is passed to the next module.
    There are also optional methods begin() and end() that can be defined.
    """

    def __init__(self, input):
        self.input = input

    def __iter__(self):
        return self

    def __next__(self):
        return self.process(next(self.input))

    def begin(self):
        if self.input:
            self.input.begin()

    def process(self, data):
        return data

    def end(self):
        if self.input:
            self.input.end()


class InputModule(Module):
    """Input Module

    Just like a module except that it generates its own input.
    It is the first module in a pipeline.
    """

    def __init__(self):
        super().__init__(None)

    def __iter__(self):
        return self

    def __next__(self):
        raise NotImplementedError("Child of InputModule must implement __next__")
