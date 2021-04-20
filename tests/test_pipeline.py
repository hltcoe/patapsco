from patapsco.pipeline import *


class MockTask1(Task):
    def process(self, item):
        return item


class MockTask2(Task):
    def process(self, item):
        return item


class DocGenerator:
    def __init__(self):
        self.docs = iter(['1', '2'])

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.docs)

    @property
    def name(self):
        return 'DocGenerator'


def test_pipeline_str():
    pipeline = StreamingPipeline(DocGenerator(), [MockTask1(), MockTask2()])
    assert str(pipeline) == 'DocGenerator | MockTask1 | MockTask2'
