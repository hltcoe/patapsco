
from patapsco.pipeline import MultiplexItem
from patapsco.retrieve import *
from patapsco.topics import Query


def test_joining_results():
    item = MultiplexItem()
    item.add("1", Results(Query("1", "en", "test"), "test system", [
        Result("doc1", 0, 9),
        Result("doc2", 1, 5),
        Result("doc3", 2, 2),
    ]))
    item.add("2", Results(Query("1", "en", "test"), "test system", [
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
