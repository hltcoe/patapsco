import pathlib
import tempfile

import pytest

from patapsco.docs import Doc
from patapsco.util.file import delete_dir
from patapsco.index import LuceneIndexer
from patapsco.retrieve import PyseriniRetriever
from patapsco.topics import Query
from patapsco.schema import IndexConfig, PathConfig, RetrieveConfig, RetrieveInputConfig


class TestLuceneIndex:
    def setup_method(self):
        self.temp_dir = pathlib.Path(tempfile.mkdtemp())

    def teardown_method(self):
        delete_dir(self.temp_dir)

    def test(self):
        run_directory = self.temp_dir
        output_directory = pathlib.Path('testIndex')
        lucene_directory = run_directory / output_directory
        conf = IndexConfig(name='lucene', output=str(output_directory))
        li = LuceneIndexer(run_path=run_directory, index_config=conf, artifact_config=conf)
        li.begin()
        li.process(Doc("1234", "unknown", "this is a test"))
        li.end()
        assert lucene_directory.exists()

    def test_two_indexes(self):
        run_directory = self.temp_dir
        output_directory = run_directory / pathlib.Path('testIndex') / 'part_0'
        conf = IndexConfig(name='lucene', output=str(output_directory))
        li = LuceneIndexer(run_path=run_directory, index_config=conf, artifact_config=conf)
        li.begin()
        li.process(Doc("1235", "unknown", "this is a another test"))
        li.writer.close()
        assert output_directory.exists()

        output_directory = run_directory / pathlib.Path('testIndex') / 'part_1'
        conf = IndexConfig(name='lucene', output=str(output_directory))
        li = LuceneIndexer(run_path=run_directory, index_config=conf, artifact_config=conf)
        li.begin()
        li.process(Doc("1234", "unknown", "this is a test"))
        li.writer.close()
        assert output_directory.exists()

        output_directory = run_directory / pathlib.Path('testIndex')
        conf = IndexConfig(name='lucene', output=str(output_directory))
        li = LuceneIndexer(run_path=run_directory, index_config=conf, artifact_config=conf)
        li.begin()
        li.process(Doc("1236", "unknown", "this is a third test"))
        li.reduce([str(output_directory / 'part_0'), str(output_directory / 'part_1')])
        li.end()
        assert output_directory.exists()

        retConfig = RetrieveConfig(
            input=RetrieveInputConfig(index=PathConfig(path=str(self.temp_dir / "testIndex"))),
            name="test",
            output="retrieve")
        retriever = PyseriniRetriever(run_path='.', config=retConfig)
        retriever.begin()
        results = retriever.process(Query('123', 'en', 'test'))
        assert len(results.results) == 3

        other_results = retriever.process(Query('124', 'en', 'another'))
        assert len(other_results.results) == 1
        assert other_results.results[0].doc_id == '1235'

        more_results = retriever.process(Query('123', 'en', 'third'))
        assert len(more_results.results) == 1
        assert more_results.results[0].doc_id == '1236'

        retriever.end()

    def test_for_errors(self):
        conf = IndexConfig(name='lucene', output=str('/testIndex'))
        with pytest.raises(PermissionError):
            li = LuceneIndexer(run_path= self.temp_dir, index_config=conf, artifact_config=conf)
        directory = self.temp_dir / 'testIndex'
        conf = IndexConfig(name='lucene', output=str(directory))
        li = LuceneIndexer(run_path=self.temp_dir, index_config=conf, artifact_config=conf)
        with pytest.raises(Exception):
            li.reduce([])
