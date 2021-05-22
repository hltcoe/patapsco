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

    def test_creating_index(self):
        run_directory = self.temp_dir
        output_directory = 'testIndex'
        lucene_directory = run_directory / output_directory
        conf = IndexConfig(name='lucene', output=output_directory)
        li = LuceneIndexer(run_path=run_directory, index_config=conf, artifact_config=conf)
        li.begin()
        li.process(Doc("1234", "eng", "this is a test"))
        li.end()
        assert lucene_directory.exists()
        lang_file = lucene_directory / ".lang"
        assert lang_file.read_text() == "eng"

    def test_two_indexes(self):
        run_directory = self.temp_dir
        output_directory = run_directory / pathlib.Path('testIndex') / 'part_0'
        conf = IndexConfig(name='lucene', output=str(output_directory))
        li = LuceneIndexer(run_path=run_directory, index_config=conf, artifact_config=conf)
        li.begin()
        li.process(Doc("1234", "eng", "this is a another test"))
        li.end()
        assert output_directory.exists()

        output_directory = run_directory / pathlib.Path('testIndex') / 'part_1'
        conf = IndexConfig(name='lucene', output=str(output_directory))
        li = LuceneIndexer(run_path=run_directory, index_config=conf, artifact_config=conf)
        li.begin()
        li.process(Doc("5678", "eng", "this is a test"))
        li.end()
        assert output_directory.exists()

        output_directory = run_directory / pathlib.Path('testIndex')
        conf = IndexConfig(name='lucene', output=str(output_directory))
        li = LuceneIndexer(run_path=run_directory, index_config=conf, artifact_config=conf)
        li.begin()
        li.reduce([str(output_directory / 'part_0'), str(output_directory / 'part_1')])
        li.end()
        assert output_directory.exists()

        ret_config = RetrieveConfig(
            input=RetrieveInputConfig(index=PathConfig(path=str(self.temp_dir / "testIndex"))),
            name="test",
            output="retrieve")
        retriever = PyseriniRetriever(run_path='.', config=ret_config)
        retriever.begin()
        results = retriever.process(Query('123', 'eng', 'test', None))
        assert len(results.results) == 2

        other_results = retriever.process(Query('124', 'eng', 'another', None))
        assert len(other_results.results) == 1
        assert other_results.results[0].doc_id == '1234'

        retriever.end()

    def test_no_permission_to_write(self):
        readonly_dir = self.temp_dir / 'readonly'
        readonly_dir.mkdir(mode=0o444)
        conf = IndexConfig(name='lucene', output=str(readonly_dir))
        li = LuceneIndexer(run_path=self.temp_dir, index_config=conf, artifact_config=conf)
        with pytest.raises(PatapscoError):
            writer = li.writer

    def test_reduce_with_bad_directory(self):
        output_dir = 'testIndex'
        conf = IndexConfig(name='lucene', output=output_dir)
        li = LuceneIndexer(run_path=self.temp_dir, index_config=conf, artifact_config=conf)
        with pytest.raises(PatapscoError):
            li.reduce([])
