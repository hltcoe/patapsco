import tempfile

import pytest

from patapsco.pipeline import MultiplexItem
from patapsco.retrieve import *
from patapsco.schema import PathConfig, RetrieveInputConfig
from patapsco.topics import Query
from patapsco.util.file import delete_dir


def test_joining_results():
    item = MultiplexItem()
    item.add("1", Results(Query("1", "en", "test", "report"), "en", "test system", [
        Result("doc1", 0, 9),
        Result("doc2", 1, 5),
        Result("doc3", 2, 2),
    ]))
    item.add("2", Results(Query("1", "en", "test", "report"), "en", "test system", [
        Result("doc4", 0, 7),
        Result("doc2", 1, 5),
        Result("doc3", 2, 1),
    ]))
    results = Joiner().process(item)
    assert results.query.id == "1"
    assert results.query.text == "test"
    assert len(results.results) == 4
    assert results.results[0].doc_id == "doc2"
    assert results.results[0].rank == 0
    assert results.results[0].score == 10


class TestPyseriniRetriever:
    def setup_method(self):
        self.temp_dir = pathlib.Path(tempfile.mkdtemp())

    def teardown_method(self):
        delete_dir(self.temp_dir)

    def test_lang(self):
        lang_path = self.temp_dir / ".lang"
        lang_path.write_text("rus")
        conf = RetrieveConfig(name="bm25", input=RetrieveInputConfig(index=PathConfig(path=str(self.temp_dir))))
        pr = PyseriniRetriever(run_path=self.temp_dir, config=conf)
        pr.begin()
        assert pr.lang == "rus"

    def test_no_lang(self):
        conf = RetrieveConfig(name="bm25", input=RetrieveInputConfig(index=PathConfig(path=str(self.temp_dir))))
        pr = PyseriniRetriever(run_path=self.temp_dir, config=conf)
        with pytest.raises(PatapscoError):
            pr.begin()
