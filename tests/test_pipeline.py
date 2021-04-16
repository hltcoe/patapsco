from patapsco.pipeline import *


class MockTask(Task):
    def process(self, item):
        return item


def test_pipeline_connect():
    a, b, c = MockTask(), MockTask(), MockTask()
    pipeline = Pipeline([a, b, c], [])
    assert pipeline.task is a
    assert pipeline.task.downstream is b
    assert pipeline.task.downstream.downstream is c
    assert pipeline.task.downstream.downstream.downstream is None
