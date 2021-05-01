import os

import pytest

from patapsco.helpers import *
from patapsco.schema import *


def test_config_preprocessor_validate():
    conf = {}
    with pytest.raises(ConfigError, match='run.name is not set'):
        ConfigPreprocessor._validate(conf)


def test_config_preprocessor_set_run_path():
    test_cases = {
        'test': 'test',
        'test space': 'test-space',
        "test's": 'tests',
    }
    for arg, ans in test_cases.items():
        conf = {'run': {'name': arg}}
        ConfigPreprocessor._set_run_path(conf)
        assert conf['run']['path'] == str(pathlib.Path('runs') / ans)


def test_config_preprocessor_set_output_paths():
    conf = {
        'run': {'name': 'run name', 'path': 'test'},
        'documents': {'db': {'path': 'docs_db'}},
        'index': {},
        'retrieve': {'output': {'path': 'initial_results'}},
    }
    ConfigPreprocessor._set_output_paths(conf)
    assert conf['documents']['output'] is False
    assert conf['documents']['db']['path'] == 'docs_db'
    assert conf['index']['output']['path'] == 'index'
    assert conf['retrieve']['output']['path'] == 'initial_results'


def test_config_preprocessor_update_relative_paths():
    conf = {
        'run': {'name': 'run name', 'path': 'test'},
        'retrieve': {'output': {'path': 'retrieve'}},
        'documents': {'db': {'path': 'database'}}
    }
    ConfigPreprocessor._update_relative_paths(conf)
    assert conf['retrieve']['output']['path'] == 'test' + os.path.sep + 'retrieve'
    assert conf['documents']['db']['path'] == 'test' + os.path.sep + 'database'


def test_config_preprocessor_update_relative_paths_with_abs_path():
    conf = {
        'run': {'name': 'run name', 'path': '/opt/test'},
        'retrieve': {'output': {'path': '/opt/patapsco/retrieve'}},
        'documents': {'db': {'path': '/opt/patapsco/database'}}
    }
    ConfigPreprocessor._update_relative_paths(conf)
    assert conf['retrieve']['output']['path'] == '/opt/patapsco/retrieve'
    assert conf['documents']['db']['path'] == '/opt/patapsco/database'


def test_config_preprocessor_update_relative_paths_with_bad_db_conf():
    conf = {
        'run': {'name': 'run name', 'path': 'test'},
        'retrieve': {'output': {'path': 'retrieve'}},
        'documents': {'database': {'path': 'database'}}
    }
    with pytest.raises(ConfigError, match='documents.db.path needs to be set'):
        ConfigPreprocessor._update_relative_paths(conf)


def test_config_preprocessor_set_retrieve_input_path_with_input_set():
    conf = {
        'run': {'name': 'run name', 'path': 'test'},
        'retrieve': {'input': {'index': {'path': 'path_to_index'}}},
    }
    ConfigPreprocessor._set_retrieve_input_path(conf)
    assert conf['retrieve']['input']['index']['path'] == 'path_to_index'


def test_config_preprocessor_set_retrieve_input_path_with_index():
    conf = {
        'run': {'name': 'run name', 'path': 'test'},
        'index': {'output': {'path': 'index'}},
        'retrieve': {},
    }
    ConfigPreprocessor._set_retrieve_input_path(conf)
    assert conf['retrieve']['input']['index']['path'] == 'index'


def test_config_preprocessor_set_retrieve_input_path_with_bad_index():
    conf = {
        'run': {'name': 'run name', 'path': 'test'},
        'index': {'output': {'notpath': 'index'}},
        'retrieve': {},
    }
    with pytest.raises(ConfigError, match='retrieve.input.index.path needs to be set'):
        ConfigPreprocessor._set_retrieve_input_path(conf)


def test_config_preprocessor_set_rerank_db_path_with_input_set():
    conf = {
        'run': {'name': 'run name', 'path': 'test'},
        'rerank': {'input': {'db': {'path': 'path_to_db'}}},
    }
    ConfigPreprocessor._set_rerank_db_path(conf)
    assert conf['rerank']['input']['db']['path'] == 'path_to_db'


def test_config_preprocessor_set_rerank_db_path_with_index():
    conf = {
        'run': {'name': 'run name', 'path': 'test'},
        'documents': {'db': {'path': 'path_to_db'}},
        'rerank': {},
    }
    ConfigPreprocessor._set_rerank_db_path(conf)
    assert conf['rerank']['input']['db']['path'] == 'path_to_db'


class TestArtitectHelper:
    def create_config(self):
        return RunnerConfig(
            run=RunConfig(name='run name', path='test'),
            documents=DocumentsConfig(
                input=DocumentsInputConfig(format="jsonl", lang="en", path="test/docs.jsonl"),
                process=TextProcessorConfig(tokenize=TokenizeConfig(name="whitespace"), stem=False),
                db=PathConfig(path="test/db"),
                output=PathConfig(path="test/docs")
            ),
            index=IndexConfig(name="mock", output=PathConfig(path="test/index")),
            topics=TopicsConfig(
                input=TopicsInputConfig(format="jsonl", lang="en", path="test/topics.jsonl"),
                output=PathConfig(path=str("test/topics"))
            ),
            queries=QueriesConfig(
                process=TextProcessorConfig(tokenize=TokenizeConfig(name="whitespace"), stem=False),
                output=PathConfig(path="test/queries")
            ),
            retrieve=RetrieveConfig(
                input=RetrieveInputConfig(index=PathConfig(path="index")),
                name="test",
                output=PathConfig(path="test/retrieve")
            ),
            rerank=RerankConfig(
                input=RerankInputConfig(db=PathConfig(path="test")),
                name="test",
                output=PathConfig(path="test/rerank")
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
