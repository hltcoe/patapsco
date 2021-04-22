from patapsco.pipeline import *


class AddTask(Task):
    def process(self, item):
        return item + 1


class MultiplyTask(Task):
    def process(self, item):
        return 2 * item

    def batch_process(self, items):
        return [3 * item for item in items]


class CollectorTask(Task):
    def __init__(self):
        self.items = []

    def process(self, item):
        self.items.append(item)


class NumberGenerator:
    def __init__(self):
        self.docs = iter(range(5))

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.docs)

    @property
    def name(self):
        return 'NumberGenerator'


def test_pipeline_str():
    pipeline = StreamingPipeline(NumberGenerator(), [AddTask(), MultiplyTask()])
    assert str(pipeline) == 'NumberGenerator | AddTask | MultiplyTask'


def test_streaming_pipeline():
    collector = CollectorTask()
    pipeline = StreamingPipeline(NumberGenerator(), [AddTask(), MultiplyTask(), collector])
    pipeline.run()
    assert pipeline.count == 5
    assert collector.items == [2, 4, 6, 8, 10]


def test_batch_pipeline():
    collector = CollectorTask()
    pipeline = BatchPipeline(NumberGenerator(), [AddTask(), MultiplyTask(), collector], 2)
    pipeline.run()
    assert pipeline.count == 5
    # the batch_process() method for multiply uses a factor of 3
    assert collector.items == [3, 6, 9, 12, 15]
