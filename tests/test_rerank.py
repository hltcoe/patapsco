import pathlib
import tempfile

import pytest

from patapsco.docs import Doc
from patapsco.rerank import *
from patapsco.results import Result
from patapsco.schema import PathConfig, RerankInputConfig
from patapsco.topics import Query
from patapsco.util.file import delete_dir


class MockDB:
    def __init__(self):
        self.path = 'test'

    def __getitem__(self, item):
        return Doc('aaa', 'eng', 'text', None)


class TestShellReranker:
    directory = (pathlib.Path(__file__).parent / 'rerank_files').absolute()

    def setup_method(self):
        self.temp_dir = tempfile.mkdtemp()
        self.path_config = PathConfig(path=self.temp_dir)

    def teardown_method(self):
        delete_dir(self.temp_dir)

    def create_config(self, script, **kwargs):
        return RerankConfig(
            input=RerankInputConfig(database=self.path_config, results=None),
            name='shell',
            script=script,
            output=self.temp_dir,
            **kwargs
        )

    def test_shell_reranker_with_success(self):
        script = str(self.directory / 'success.sh')
        config = self.create_config(script)
        reranker = ShellReranker(run_path='', config=config, db=MockDB())
        items = [Results(Query('1', 'eng', 'text', 'text', 'report'), 'eng', 'test', [
            Result('aaa', 1, 0.5),
            Result('bbb', 2, 0.4)
        ])]
        new_items = reranker.batch_process(items)
        assert new_items[0].query.id == '1'
        assert new_items[0].query.lang == 'eng'
        assert new_items[0].results[0].doc_id == 'bbb'
        assert new_items[0].results[1].doc_id == 'aaa'
        assert reranker.batch == 1

    def test_shell_reranker_with_fewer_queries_in_output(self):
        script = str(self.directory / 'success.sh')
        config = self.create_config(script)
        reranker = ShellReranker(run_path='', config=config, db=MockDB())
        items = [
            Results(Query('1', 'eng', 'text', 'text', 'report 1'), 'eng', 'test', [Result('aaa', 1, 0.5), Result('bbb', 2, 0.4)]),
            Results(Query('2', 'eng', 'text2', 'text 2', 'report 2'), 'eng', 'test', [Result('aaa', 1, 0.5), Result('bbb', 2, 0.4)]),
        ]
        with pytest.raises(PatapscoError):
            reranker.batch_process(items)

    def test_shell_reranker_with_error(self):
        script = str(self.directory / 'error.sh')
        config = self.create_config(script)
        reranker = ShellReranker(run_path='', config=config, db=MockDB())
        items = [Results(Query('1', 'eng', 'text', 'text', 'report'), 'eng', 'test', [Result('1', 1, 0.5)])]
        with pytest.raises(PatapscoError):
            reranker.batch_process(items)

    def test_shell_reranker_with_no_exist_script(self):
        script = str(self.directory / 'nothing.sh')
        config = self.create_config(script)
        with pytest.raises(ConfigError):
            ShellReranker(run_path='', config=config, db=MockDB())

    def test_shell_reranker_call_process(self):
        script = str(self.directory / 'success.sh')
        config = self.create_config(script)
        reranker = ShellReranker(run_path='', config=config, db=MockDB())
        item = Results(Query('1', 'eng', 'text', 'text', 'report'), 'eng', 'test', [
            Result('aaa', 1, 0.5),
            Result('bbb', 2, 0.4)
        ])
        with pytest.raises(ConfigError):
            reranker.process(item)

    def test_shell_reranker_with_args(self):
        script = str(self.directory / 'args.sh')
        config = self.create_config(script, embedding="mbert")
        reranker = ShellReranker(run_path='', config=config, db=MockDB())
        items = [Results(Query('1', 'eng', 'text', 'text', 'report'), 'eng', 'test', [
            Result('aaa', 1, 0.5),
            Result('bbb', 2, 0.4)
        ])]
        new_items = reranker.batch_process(items)
        assert new_items[0].query.id == '1'
        assert new_items[0].query.lang == 'eng'
        assert new_items[0].results[0].doc_id == 'bbb'
        assert new_items[0].results[1].doc_id == 'aaa'
        assert reranker.batch == 1
