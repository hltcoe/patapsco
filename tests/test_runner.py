import os

import pytest

from patapsco.config import PathConfig
from patapsco.docs import DocumentsInputConfig, DocumentsProcessorConfig
from patapsco.runner import *
from patapsco.text import TokenizeConfig
from patapsco.topics import TopicsInputConfig


def test_config_preprocessor_validate():
    conf = {}
    with pytest.raises(ConfigError, match='run.path is not set'):
        ConfigPreprocessor._validate(conf)


def test_config_preprocessor_set_output_paths():
    conf = {
        'run': {'path': 'test'},
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
        'run': {'path': 'test'},
        'retrieve': {'output': {'path': 'retrieve'}},
        'documents': {'db': {'path': 'database'}}
    }
    ConfigPreprocessor._update_relative_paths(conf)
    assert conf['retrieve']['output']['path'] == 'test' + os.path.sep + 'retrieve'
    assert conf['documents']['db']['path'] == 'test' + os.path.sep + 'database'


def test_config_preprocessor_update_relative_paths_with_abs_path():
    conf = {
        'run': {'path': '/opt/test'},
        'retrieve': {'output': {'path': '/opt/patapsco/retrieve'}},
        'documents': {'db': {'path': '/opt/patapsco/database'}}
    }
    ConfigPreprocessor._update_relative_paths(conf)
    assert conf['retrieve']['output']['path'] == '/opt/patapsco/retrieve'
    assert conf['documents']['db']['path'] == '/opt/patapsco/database'


def test_config_preprocessor_update_relative_paths_with_bad_db_conf():
    conf = {
        'run': {'path': 'test'},
        'retrieve': {'output': {'path': 'retrieve'}},
        'documents': {'database': {'path': 'database'}}
    }
    with pytest.raises(ConfigError, match='documents.db.path needs to be set'):
        ConfigPreprocessor._update_relative_paths(conf)


def test_config_preprocessor_set_retrieve_input_path_with_input_set():
    conf = {
        'run': {'path': 'test'},
        'retrieve': {'input': {'index': {'path': 'path_to_index'}}},
    }
    ConfigPreprocessor._set_retrieve_input_path(conf)
    assert conf['retrieve']['input']['index']['path'] == 'path_to_index'


def test_config_preprocessor_set_retrieve_input_path_with_index():
    conf = {
        'run': {'path': 'test'},
        'index': {'output': {'path': 'index'}},
        'retrieve': {},
    }
    ConfigPreprocessor._set_retrieve_input_path(conf)
    assert conf['retrieve']['input']['index']['path'] == 'index'


def test_config_preprocessor_set_retrieve_input_path_with_bad_index():
    conf = {
        'run': {'path': 'test'},
        'index': {'output': {'notpath': 'index'}},
        'retrieve': {},
    }
    with pytest.raises(ConfigError, match='retrieve.input.index.path needs to be set'):
        ConfigPreprocessor._set_retrieve_input_path(conf)


def test_config_preprocessor_set_rerank_db_path_with_input_set():
    conf = {
        'run': {'path': 'test'},
        'rerank': {'input': {'db': {'path': 'path_to_db'}}},
    }
    ConfigPreprocessor._set_rerank_db_path(conf)
    assert conf['rerank']['input']['db']['path'] == 'path_to_db'


def test_config_preprocessor_set_rerank_db_path_with_index():
    conf = {
        'run': {'path': 'test'},
        'documents': {'db': {'path': 'path_to_db'}},
        'rerank': {},
    }
    ConfigPreprocessor._set_rerank_db_path(conf)
    assert conf['rerank']['input']['db']['path'] == 'path_to_db'


def test_partial_config_preparer():
    directory = pathlib.Path(__file__).parent / 'config_files'
    path = directory / 'full_config.yml'
    conf = ConfigPreprocessor.process(path, {})
    helper = ArtifactHelper()
    artifact_conf = helper.get_config(conf, Tasks.DOCUMENTS)
    assert hasattr(artifact_conf, 'documents')
    assert not hasattr(artifact_conf, 'topics')
    assert not hasattr(artifact_conf, 'index')
    assert not hasattr(artifact_conf, 'score')


class TestPipelineBuilder:
    dir = pathlib.Path(__file__).parent / 'plan_files'

    def create_config(self, path):
        return RunnerConfig(
            run=RunConfig(path=str(self.dir / path)),
            documents=DocumentsConfig(
                input=DocumentsInputConfig(format="trec", lang="en", path="test"),
                process=DocumentsProcessorConfig(tokenize=TokenizeConfig(name="test"), stem=False),
                db=PathConfig(path="test"),
                output=PathConfig(path=str(self.dir / path / "docs"))
            ),
            index=IndexConfig(name="test", output=PathConfig(path=str(self.dir / path / "index"))),
            topics=TopicsConfig(
                input=TopicsInputConfig(format="trec", lang="en", path="test"), output=False
            )
        )

    def test_create_plan_with_no_stages(self):
        conf = RunnerConfig(run=RunConfig(path="test"))
        builder = PipelineBuilder(conf)
        with pytest.raises(ConfigError, match='No tasks are configured to run'):
            builder.create_plan()

    def test_create_plan_with_normal_stage1(self):
        conf = self.create_config('test')
        builder = PipelineBuilder(conf)
        stage1_plan, stage2_plan = builder.create_plan()
        assert Tasks.DOCUMENTS in stage1_plan
        assert Tasks.INDEX in stage1_plan

    def test_create_plan_with_completed_documents(self):
        conf = self.create_config('completed_docs')
        builder = PipelineBuilder(conf)
        stage1_plan, _ = builder.create_plan()
        assert Tasks.DOCUMENTS not in stage1_plan
        assert Tasks.INDEX in stage1_plan

    def test_create_plan_with_incomplete_documents(self):
        conf = self.create_config('incomplete_docs')
        builder = PipelineBuilder(conf)
        stage1_plan, _ = builder.create_plan()
        assert Tasks.DOCUMENTS in stage1_plan
        assert Tasks.INDEX in stage1_plan

    def test_create_plan_with_completed_documents_and_completed_index(self):
        conf = self.create_config('stage1_completed')
        builder = PipelineBuilder(conf)
        stage1_plan, _ = builder.create_plan()
        assert stage1_plan == []

    def test_create_plan_with_incomplete_documents_and_completed_index(self):
        conf = self.create_config('docs_incomplete_index_complete')
        builder = PipelineBuilder(conf)
        stage1_plan, _ = builder.create_plan()
        assert stage1_plan == []
