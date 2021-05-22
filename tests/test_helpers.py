import pytest

from patapsco.helpers import *
from patapsco.schema import *


class TestConfigHelper:
    def test_validate(self):
        conf = {}
        with pytest.raises(ConfigError, match='run.name is not set'):
            ConfigHelper._validate(conf)

    def test_set_run_path(self):
        test_cases = {
            'test': 'test',
            'test space': 'test-space',
            "test's": 'tests',
        }
        for arg, ans in test_cases.items():
            conf = {'run': {'name': arg}}
            ConfigHelper._set_run_path(conf)
            assert conf['run']['path'] == str(pathlib.Path('runs') / ans)

    def test_set_output_paths(self):
        conf = RunnerConfig(
            run=RunConfig(name='test'),
            documents=DocumentsConfig(
                input=DocumentsInputConfig(format='jsonl', lang='eng', path='test'),
                process=TextProcessorConfig(tokenize='whitespace'),
            ),
            index=IndexConfig(name='mock')
        )
        ConfigHelper._set_output_paths(conf)
        assert conf.documents.output is False
        assert conf.database.output == 'database'
        assert conf.index.output == 'index'

    def test_set_retrieve_input_path_with_input_set(self):
        conf = RunnerConfig(
            run=RunConfig(name='test'),
            retrieve=RetrieveConfig(
                input=RetrieveInputConfig(index=PathConfig(path='path_to_index')),
                name='mock'
            )
        )
        ConfigHelper._set_retrieve_input_path(conf)
        assert conf.retrieve.input.index.path == 'path_to_index'

    def test_set_retrieve_input_path_with_input_not_set(self):
        conf = RunnerConfig(
            run=RunConfig(name='test'),
            index=IndexConfig(
                name='mock',
                output='index'
            ),
            retrieve=RetrieveConfig(
                name='mock'
            )
        )
        ConfigHelper._set_retrieve_input_path(conf)
        assert conf.retrieve.input.index.path == 'index'

    def test_set_retrieve_input_path_with_missing_index(self):
        conf = RunnerConfig(
            run=RunConfig(name='test'),
            retrieve=RetrieveConfig(
                name='mock'
            )
        )
        with pytest.raises(ConfigError, match='retrieve.input.index.path needs to be set'):
            ConfigHelper._set_retrieve_input_path(conf)

    def test_set_rerank_db_path_with_input_set(self):
        conf = RunnerConfig(
            run=RunConfig(name='test'),
            rerank=RerankConfig(
                input=RerankInputConfig(db=PathConfig(path='path_to_db')),
                name='mock'
            )
        )
        ConfigHelper._set_rerank_db_path(conf)
        assert conf.rerank.input.db.path == 'path_to_db'

    def test_set_rerank_db_path_with_index(self):
        conf = RunnerConfig(
            run=RunConfig(name='test'),
            database=DatabaseConfig(output='db_path'),
            rerank=RerankConfig(
                name='mock'
            )
        )
        ConfigHelper._set_rerank_db_path(conf)
        assert conf.rerank.input.db.path == 'db_path'


class TestArtifactHelper:
    def create_config(self):
        return RunnerConfig(
            run=RunConfig(name='run name', path='test'),
            database=DatabaseConfig(output="database"),
            documents=DocumentsConfig(
                input=DocumentsInputConfig(format="jsonl", lang="eng", path="test/docs.jsonl"),
                process=TextProcessorConfig(tokenize="whitespace", stem=False),
                output="docs"
            ),
            index=IndexConfig(name="mock", output="index"),
            topics=TopicsConfig(
                input=TopicsInputConfig(format="jsonl", lang="eng", path="test/topics.jsonl"),
                output="topics"
            ),
            queries=QueriesConfig(
                process=TextProcessorConfig(tokenize="whitespace", stem=False),
                output="queries"
            ),
            retrieve=RetrieveConfig(
                input=RetrieveInputConfig(index=PathConfig(path="index")),
                name="test",
                output="retrieve"
            ),
            rerank=RerankConfig(
                input=RerankInputConfig(db=PathConfig(path="test")),
                name="test",
                output="rerank"
            ),
            score=ScoreConfig(input=ScoreInputConfig(format='trec', path='qrels'))
        )

    def test_get_config_documents(self):
        helper = ArtifactHelper()
        conf = helper.get_config(self.create_config(), Tasks.DOCUMENTS)
        assert hasattr(conf, 'run')
        assert hasattr(conf, 'documents')
        assert not hasattr(conf, 'index')

    def test_get_config_index(self):
        helper = ArtifactHelper()
        conf = helper.get_config(self.create_config(), Tasks.INDEX)
        assert hasattr(conf, 'run')
        assert hasattr(conf, 'documents')
        assert hasattr(conf, 'index')
        assert not hasattr(conf, 'topics')

    def test_get_config_topics(self):
        helper = ArtifactHelper()
        conf = helper.get_config(self.create_config(), Tasks.TOPICS)
        assert hasattr(conf, 'run')
        assert not hasattr(conf, 'documents')
        assert not hasattr(conf, 'index')
        assert hasattr(conf, 'topics')
        assert not hasattr(conf, 'queries')

    def test_get_config_queries(self):
        helper = ArtifactHelper()
        conf = helper.get_config(self.create_config(), Tasks.QUERIES)
        assert hasattr(conf, 'run')
        assert not hasattr(conf, 'documents')
        assert not hasattr(conf, 'index')
        assert hasattr(conf, 'topics')
        assert hasattr(conf, 'queries')
        assert not hasattr(conf, 'retrieve')

    def test_get_config_retrieve(self):
        helper = ArtifactHelper()
        conf = helper.get_config(self.create_config(), Tasks.RETRIEVE)
        assert hasattr(conf, 'run')
        assert hasattr(conf, 'documents')
        assert hasattr(conf, 'index')
        assert hasattr(conf, 'topics')
        assert hasattr(conf, 'queries')
        assert hasattr(conf, 'retrieve')
        assert not hasattr(conf, 'rerank')

    def test_get_config_rerank(self):
        helper = ArtifactHelper()
        conf = helper.get_config(self.create_config(), Tasks.RERANK)
        assert hasattr(conf, 'run')
        assert hasattr(conf, 'documents')
        assert hasattr(conf, 'index')
        assert hasattr(conf, 'topics')
        assert hasattr(conf, 'queries')
        assert hasattr(conf, 'retrieve')
        assert hasattr(conf, 'rerank')
