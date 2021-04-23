import os

import pytest

from patapsco.config import PathConfig
from patapsco.docs import DocumentsInputConfig
from patapsco.rerank import RerankInputConfig
from patapsco.retrieve import RetrieveInputConfig
from patapsco.runner import *
from patapsco.score import ScoreInputConfig
from patapsco.text import TextProcessorConfig, TokenizeConfig
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
    dir = pathlib.Path(__file__).parent

    def create_config(self, path):
        output_directory = self.dir / "build_files" / "output" / path
        input_directory = self.dir / "build_files" / "input"
        return RunnerConfig(
            run=RunConfig(path=str(output_directory)),
            documents=DocumentsConfig(
                input=DocumentsInputConfig(format="jsonl", lang="en", path=str(input_directory / "docs.jsonl")),
                process=TextProcessorConfig(tokenize=TokenizeConfig(name="test"), stem=False),
                db=PathConfig(path="test"),
                output=PathConfig(path=str(output_directory / "docs"))
            ),
            index=IndexConfig(name="test", output=PathConfig(path=str(output_directory / "index"))),
            topics=TopicsConfig(
                input=TopicsInputConfig(format="jsonl", lang="en", path=str(input_directory / "topics.jsonl")),
                output=PathConfig(path=str(output_directory / "topics"))
            ),
            queries=QueriesConfig(
                process=TextProcessorConfig(tokenize=TokenizeConfig(name="test"), stem=False),
                output=PathConfig(path=str(output_directory / "queries"))
            ),
            retrieve=RetrieveConfig(
                input=RetrieveInputConfig(index=PathConfig(path="index")),
                name="test",
                output=PathConfig(path=str(output_directory / "retrieve"))
            ),
            rerank=RerankConfig(
                input=RerankInputConfig(db=PathConfig(path="test")),
                name="test",
                output=PathConfig(path=str(output_directory / "rerank"))
            ),
            score=ScoreConfig(input=ScoreInputConfig(format='trec', path=str(input_directory / 'qrels')))
        )

    def test_create_plan_with_no_stages(self):
        conf = RunnerConfig(run=RunConfig(path="test"))
        builder = PipelineBuilder(conf)
        with pytest.raises(ConfigError, match='No tasks are configured to run'):
            builder.create_plan()

    def test_create_plan_with_normal_config(self):
        conf = self.create_config('test')
        builder = PipelineBuilder(conf)
        stage1_plan, stage2_plan = builder.create_plan()
        assert Tasks.DOCUMENTS in stage1_plan
        assert Tasks.INDEX in stage1_plan
        assert Tasks.TOPICS in stage2_plan
        assert Tasks.QUERIES in stage2_plan
        assert Tasks.RETRIEVE in stage2_plan
        assert Tasks.RERANK in stage2_plan
        assert Tasks.SCORE in stage2_plan

    def test_create_plan_with_complete_documents(self):
        conf = self.create_config('complete_docs')
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

    def test_create_plan_with_complete_documents_and_complete_index(self):
        conf = self.create_config('stage1_complete')
        builder = PipelineBuilder(conf)
        stage1_plan, _ = builder.create_plan()
        assert stage1_plan == []

    def test_create_plan_with_incomplete_documents_and_complete_index(self):
        conf = self.create_config('docs_incomplete_index_complete')
        builder = PipelineBuilder(conf)
        stage1_plan, _ = builder.create_plan()
        assert stage1_plan == []

    def test_create_plan_with_complete_topics(self):
        conf = self.create_config('topics_complete')
        builder = PipelineBuilder(conf)
        _, stage2_plan = builder.create_plan()
        assert Tasks.TOPICS not in stage2_plan
        assert Tasks.QUERIES in stage2_plan

    def test_create_plan_with_complete_queries(self):
        conf = self.create_config('queries_complete')
        builder = PipelineBuilder(conf)
        _, stage2_plan = builder.create_plan()
        assert Tasks.TOPICS not in stage2_plan
        assert Tasks.QUERIES not in stage2_plan
        assert Tasks.RETRIEVE in stage2_plan

    def test_create_plan_with_complete_retrieval(self):
        conf = self.create_config('retrieval_complete')
        builder = PipelineBuilder(conf)
        _, stage2_plan = builder.create_plan()
        assert Tasks.TOPICS not in stage2_plan
        assert Tasks.QUERIES not in stage2_plan
        assert Tasks.RETRIEVE not in stage2_plan
        assert Tasks.RERANK in stage2_plan

    def test_create_plan_with_complete_rerank(self):
        conf = self.create_config('rerank_complete')
        builder = PipelineBuilder(conf)
        with pytest.raises(ConfigError, match="Rerank is already complete. Delete its output directory to rerun reranking."):
            builder.create_plan()

    def test_create_plan_with_no_rerank_retrieval(self):
        conf = self.create_config('test')
        conf.rerank = None
        conf.retrieve = None
        builder = PipelineBuilder(conf)
        with pytest.raises(ConfigError, match="Scorer can only run if either retrieve or rerank is configured."):
            builder.create_plan()

    def test_build_stage1_with_standard_docs(self):
        conf = self.create_config('test')
        builder = PipelineBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        pipeline = builder.build_stage1(plan)
        assert isinstance(pipeline.tasks[0].task, DocumentProcessor)

    def test_build_stage1_with_bad_doc_format(self):
        conf = self.create_config('test')
        conf.documents.input.format = "abc"
        builder = PipelineBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        with pytest.raises(ConfigError, match="Unknown input document type: abc"):
            builder.build_stage1(plan)

    def test_build_stage1_with_no_files(self):
        conf = self.create_config('test')
        conf.documents.input.path = "nothing"
        builder = PipelineBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        with pytest.raises(ConfigError, match="No files match pattern"):
            builder.build_stage1(plan)
