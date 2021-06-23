import pathlib
import tempfile

import pytest

from patapsco.docs import Doc
from patapsco.error import PatapscoError
from patapsco.index import LuceneIndexer
from patapsco.retrieve import PyseriniRetriever
from patapsco.schema import IndexConfig, PathConfig, RetrieveConfig, RetrieveInputConfig
from patapsco.topics import Query
from patapsco.util.file import delete_dir


class TestLuceneIndex:
    def setup_method(self):
        self.temp_dir = pathlib.Path(tempfile.mkdtemp())

    def teardown_method(self):
        delete_dir(self.temp_dir)

    def test_4_docs(self):
        run_directory = self.temp_dir
        output_directory = 'testIndex'
        lucene_directory = run_directory / output_directory
        conf = IndexConfig(name='lucene', output=output_directory)
        li = LuceneIndexer(run_path=run_directory, index_config=conf, artifact_config=conf)
        li.begin()
        li.process(Doc("1", "eng", "gato felino", None))
        li.process(Doc("2", "eng", "gato and some extra words gato", None))
        li.process(Doc("3", "eng", "felino", None))
        li.process(Doc("4", "eng", "gato", None))
        li.end()
        assert lucene_directory.exists()
        lang_file = lucene_directory / ".lang"
        assert lang_file.read_text() == "eng"

    # def test_two_indexes(self):
    #     run_directory = self.temp_dir
    #     output_directory = run_directory / pathlib.Path('testIndex') / 'part_0'
    #     conf = IndexConfig(name='lucene', output=str(output_directory))
    #     li = LuceneIndexer(run_path=run_directory, index_config=conf, artifact_config=conf)
    #     li.begin()
    #     li.process(Doc("1234", "eng", "this is a another test", None))
    #     li.end()
    #     assert output_directory.exists()
    #
    #     output_directory = run_directory / pathlib.Path('testIndex') / 'part_1'
    #     conf = IndexConfig(name='lucene', output=str(output_directory))
    #     li = LuceneIndexer(run_path=run_directory, index_config=conf, artifact_config=conf)
    #     li.begin()
    #     li.process(Doc("5678", "eng", "this is a test", None))
    #     li.end()
    #     assert output_directory.exists()
    #
    #     output_directory = run_directory / pathlib.Path('testIndex')
    #     conf = IndexConfig(name='lucene', output=str(output_directory))
    #     li = LuceneIndexer(run_path=run_directory, index_config=conf, artifact_config=conf)
    #     li.begin()
    #     li.reduce([str(output_directory / 'part_0'), str(output_directory / 'part_1')])
    #     li.end()
    #     assert output_directory.exists()
    #
        ret_config = RetrieveConfig(
            input=RetrieveInputConfig(index=PathConfig(path=str(self.temp_dir / "testIndex"))),
            name="psq",
            output="retrieve")
        retriever = PyseriniRetriever(run_path='.', config=ret_config)
        retriever.begin()
        results = retriever.process(Query('123', 'eng', '(gato^0.8 felino^0.2) AND (extra^0.9 words^0.1)', 'test', None))
        assert len(results.results) == 4
        assert '2' == results.results[0].doc_id
        assert pytest.approx(0.5117189, results.results[0].score, 1e-5)


    #
    #     other_results = retriever.process(Query('124', 'eng', 'another', 'test', None))
    #     assert len(other_results.results) == 1
    #     assert other_results.results[0].doc_id == '1234'
    #
        retriever.end()
