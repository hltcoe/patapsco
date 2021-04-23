import os
import tempfile

import pytest

from patapsco.config import PathConfig
from patapsco.docs import DocumentsInputConfig
from patapsco.index import IndexInputConfig, MockIndexer
from patapsco.rerank import RerankInputConfig
from patapsco.retrieve import RetrieveInputConfig, MockRetriever
from patapsco.runner import *
from patapsco.score import ScoreInputConfig
from patapsco.text import TextProcessorConfig, TokenizeConfig
from patapsco.topics import TopicsInputConfig, QueriesInputConfig


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
    # TODO no tests for multiplexing index yet
    dir = pathlib.Path(__file__).parent

    def setup_method(self):
        self.temp_dir = pathlib.Path(tempfile.mkdtemp())

    def teardown_method(self):
        delete_dir(self.temp_dir)
        path = self.dir / "build_files" / "output" / "docs_complete" / "index"
        if path.exists():
            delete_dir(path)
        path = self.dir / "build_files" / "output" / "topics_complete" / "queries"
        if path.exists():
            delete_dir(path)

    def create_config(self, path):
        output_directory = self.dir / "build_files" / "output" / path
        input_directory = self.dir / "build_files" / "input"
        if path == 'test':
            output_directory = self.temp_dir
        return RunnerConfig(
            run=RunConfig(path=str(output_directory)),
            documents=DocumentsConfig(
                input=DocumentsInputConfig(format="jsonl", lang="en", path=str(input_directory / "docs.jsonl")),
                process=TextProcessorConfig(tokenize=TokenizeConfig(name="whitespace"), stem=False),
                db=PathConfig(path=str(self.temp_dir / "db")),
                output=PathConfig(path=str(output_directory / "docs"))
            ),
            index=IndexConfig(name="mock", output=PathConfig(path=str(output_directory / "index"))),
            topics=TopicsConfig(
                input=TopicsInputConfig(format="jsonl", lang="en", path=str(input_directory / "topics.jsonl")),
                output=PathConfig(path=str(output_directory / "topics"))
            ),
            queries=QueriesConfig(
                process=TextProcessorConfig(tokenize=TokenizeConfig(name="whitespace"), stem=False),
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
        conf = RunnerConfig(run=RunConfig(path=str(self.temp_dir)))
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
        conf = self.create_config('docs_complete')
        builder = PipelineBuilder(conf)
        stage1_plan, _ = builder.create_plan()
        assert Tasks.DOCUMENTS not in stage1_plan
        assert Tasks.INDEX in stage1_plan

    def test_create_plan_with_incomplete_documents(self):
        conf = self.create_config('docs_incomplete')
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
        assert len(pipeline.tasks) == 2
        assert isinstance(pipeline.tasks[0].task, DocumentProcessor)
        assert isinstance(pipeline.tasks[1].task, DocWriter)

    def test_build_stage1_with_standard_docs_but_no_output(self):
        conf = self.create_config('test')
        conf.documents.output = False
        builder = PipelineBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        pipeline = builder.build_stage1(plan)
        assert len(pipeline.tasks) == 1
        assert isinstance(pipeline.tasks[0].task, DocumentProcessor)

    def test_build_stage1_with_bad_doc_format(self):
        conf = self.create_config('test')
        conf.documents.input.format = "abc"
        builder = PipelineBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        with pytest.raises(ConfigError, match="Unknown input document type: abc"):
            builder.build_stage1(plan)

    def test_build_stage1_with_bad_doc_encoding(self):
        conf = self.create_config('test')
        conf.documents.input.encoding = "abc"
        builder = PipelineBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        with pytest.raises(ConfigError, match="abc is not a valid file encoding"):
            builder.build_stage1(plan)

    def test_build_stage1_with_no_files(self):
        conf = self.create_config('test')
        conf.documents.input.path = "nothing"
        builder = PipelineBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        with pytest.raises(ConfigError, match="No files match pattern"):
            builder.build_stage1(plan)

    def test_build_stage1_with_bad_lang(self):
        conf = self.create_config('test')
        conf.documents.input.lang = "da"
        builder = PipelineBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        with pytest.raises(ConfigError, match="Unknown language code: da"):
            builder.build_stage1(plan)

    def test_build_stage1_with_bad_tokenizer(self):
        conf = self.create_config('test')
        conf.documents.process.tokenize.name = "nothing"
        builder = PipelineBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        with pytest.raises(ConfigError):
            builder.build_stage1(plan)

    def test_build_stage1_with_standard_docs_split(self):
        conf = self.create_config('test')
        conf.documents.process.splits = ['tokenize', 'tokenize+lowercase']
        builder = PipelineBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        pipeline = builder.build_stage1(plan)
        assert len(pipeline.tasks) == 2
        assert isinstance(pipeline.tasks[0].task, DocumentProcessor)
        assert pipeline.tasks[0].task.splitter.splits == {'tokenize': 'tokenize', 'lowercase': 'tokenize+lowercase'}
        assert isinstance(pipeline.tasks[1].task, MultiplexTask)

    def test_build_stage1_with_standard_docs_and_bad_split(self):
        conf = self.create_config('test')
        conf.documents.process.splits = ['tokenize', 'tokenize+uppercase']
        builder = PipelineBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        with pytest.raises(ConfigError, match="Unrecognized split"):
            builder.build_stage1(plan)

    def test_build_stage1_with_no_documents_for_indexer(self):
        conf = self.create_config('test')
        conf.documents = None
        builder = PipelineBuilder(conf)
        plan = [Tasks.INDEX]
        with pytest.raises(ConfigError, match="index not configured with documents"):
            builder.build_stage1(plan)

    def test_build_stage1_with_indexer_gets_docs_from_documents(self):
        conf = self.create_config('docs_complete')
        builder = PipelineBuilder(conf)
        plan = [Tasks.INDEX]
        pipeline = builder.build_stage1(plan)
        assert isinstance(pipeline.tasks[0].task, MockIndexer)

    def test_build_stage1_with_indexer_gets_docs_from_input(self):
        conf = self.create_config('docs_complete')
        conf.documents = None
        conf.index.input = IndexInputConfig(documents=PathConfig(path=str(self.dir / "build_files" / "output" / "docs_complete" / "docs")))
        builder = PipelineBuilder(conf)
        plan = [Tasks.INDEX]
        pipeline = builder.build_stage1(plan)
        assert isinstance(pipeline.tasks[0].task, MockIndexer)

    def test_build_stage1_with_indexer_gets_bad_docs_from_input(self):
        conf = self.create_config('test')
        conf.documents = None
        conf.index.input = IndexInputConfig(documents=PathConfig(path=str(self.dir / "build_files" / "output" / "docs_incomplete" / "docs")))
        builder = PipelineBuilder(conf)
        plan = [Tasks.INDEX]
        with pytest.raises(ConfigError, match="Unable to load artifact config"):
            builder.build_stage1(plan)

    def test_build_stage1_with_standard_docs_index(self):
        conf = self.create_config('test')
        builder = PipelineBuilder(conf)
        plan = [Tasks.DOCUMENTS, Tasks.INDEX]
        pipeline = builder.build_stage1(plan)
        assert len(pipeline.tasks) == 3
        assert isinstance(pipeline.tasks[0].task, DocumentProcessor)
        assert isinstance(pipeline.tasks[1].task, DocWriter)
        assert isinstance(pipeline.tasks[2].task, MockIndexer)

    def test_build_stage1_with_bad_pipeline_mode(self):
        conf = self.create_config('test')
        conf.run.stage1.mode = "fast"
        builder = PipelineBuilder(conf)
        plan = [Tasks.DOCUMENTS, Tasks.INDEX]
        with pytest.raises(ConfigError, match="Unrecognized pipeline mode"):
            builder.build_stage1(plan)

    def test_build_stage2_with_standard_topics(self):
        conf = self.create_config('test')
        builder = PipelineBuilder(conf)
        plan = [Tasks.TOPICS]
        pipeline = builder.build_stage2(plan)
        assert len(pipeline.tasks) == 2
        assert isinstance(pipeline.tasks[0].task, TopicProcessor)
        assert isinstance(pipeline.tasks[1].task, QueryWriter)

    def test_build_stage2_with_standard_topics_but_no_output(self):
        conf = self.create_config('test')
        conf.topics.output = False
        builder = PipelineBuilder(conf)
        plan = [Tasks.TOPICS]
        pipeline = builder.build_stage2(plan)
        assert len(pipeline.tasks) == 1
        assert isinstance(pipeline.tasks[0].task, TopicProcessor)

    def test_build_stage2_with_bad_topic_format(self):
        conf = self.create_config('test')
        conf.topics.input.format = "abc"
        builder = PipelineBuilder(conf)
        plan = [Tasks.TOPICS]
        with pytest.raises(ConfigError, match="Unknown topic type: abc"):
            builder.build_stage2(plan)

    def test_build_stage2_with_bad_encoding(self):
        conf = self.create_config('test')
        conf.topics.input.encoding = "abc"
        builder = PipelineBuilder(conf)
        plan = [Tasks.TOPICS]
        with pytest.raises(ConfigError, match="abc is not a valid file encoding"):
            builder.build_stage2(plan)

    def test_build_stage2_with_no_files(self):
        conf = self.create_config('test')
        conf.topics.input.path = "nothing"
        builder = PipelineBuilder(conf)
        plan = [Tasks.TOPICS]
        with pytest.raises(ConfigError, match="No files match pattern"):
            builder.build_stage2(plan)

    def test_build_stage2_with_bad_lang(self):
        conf = self.create_config('test')
        conf.topics.input.lang = "da"
        builder = PipelineBuilder(conf)
        plan = [Tasks.TOPICS]
        with pytest.raises(ConfigError, match="Unknown language code: da"):
            builder.build_stage2(plan)

    def test_build_stage2_queries_with_no_topics(self):
        conf = self.create_config('test')
        conf.topics = None
        builder = PipelineBuilder(conf)
        plan = [Tasks.QUERIES]
        with pytest.raises(ConfigError, match="query processor not configured with input"):
            builder.build_stage2(plan)

    def test_build_stage2_queries_with_topic_output(self):
        conf = self.create_config('topics_complete')
        builder = PipelineBuilder(conf)
        plan = [Tasks.QUERIES]
        pipeline = builder.build_stage2(plan)
        assert isinstance(pipeline.tasks[0].task, QueryProcessor)

    def test_build_stage2_queries_with_query_input(self):
        conf = self.create_config('topics_complete')
        conf.topics = None
        conf.queries.input = QueriesInputConfig(path=str(self.dir / "build_files" / "output" / "topics_complete" / "topics"))
        builder = PipelineBuilder(conf)
        plan = [Tasks.QUERIES]
        pipeline = builder.build_stage2(plan)
        assert isinstance(pipeline.tasks[0].task, QueryProcessor)

    def test_build_stage2_queries_with_query_input_to_file(self):
        conf = self.create_config('topics_complete')
        conf.topics = None
        conf.queries.input = QueriesInputConfig(path=str(self.dir / "build_files" / "output" / "topics_complete" / "topics" / "queries.jsonl"))
        builder = PipelineBuilder(conf)
        plan = [Tasks.QUERIES]
        pipeline = builder.build_stage2(plan)
        assert isinstance(pipeline.tasks[0].task, QueryProcessor)
        assert pipeline.tasks[0].task.lang == "en"

    def test_check_sources_of_documents_standard_config(self):
        conf = self.create_config('test')
        builder = PipelineBuilder(conf)
        builder.check_sources_of_documents()

    def test_check_sources_of_documents_str_same(self):
        db_dir = pathlib.Path(__file__).parent / "build_files" / "output" / "database"
        conf = self.create_config('test')
        conf.documents.input.path = "docs.jsonl"
        conf.rerank.input.db.path = str(db_dir)
        builder = PipelineBuilder(conf)
        builder.check_sources_of_documents()

    def test_check_sources_of_documents_str_diff(self):
        db_dir = pathlib.Path(__file__).parent / "build_files" / "output" / "database"
        conf = self.create_config('test')
        conf.documents.input.path = "docs_other_data.jsonl"
        conf.rerank.input.db.path = str(db_dir)
        builder = PipelineBuilder(conf)
        with pytest.raises(ConfigError):
            builder.check_sources_of_documents()

    def test_check_sources_of_documents_type_diff(self):
        db_dir = pathlib.Path(__file__).parent / "build_files" / "output" / "database"
        conf = self.create_config('test')
        conf.documents.input.path = ["docs_other_data.jsonl", "more_docs.jsonl"]
        conf.rerank.input.db.path = str(db_dir)
        builder = PipelineBuilder(conf)
        with pytest.raises(ConfigError):
            builder.check_sources_of_documents()

    def test_check_sources_of_documents_list_same(self):
        db_dir = pathlib.Path(__file__).parent / "build_files" / "output" / "database_doc_list"
        conf = self.create_config('test')
        conf.documents.input.path = ["docs.jsonl", "docs2.jsonl"]
        conf.rerank.input.db.path = str(db_dir)
        builder = PipelineBuilder(conf)
        builder.check_sources_of_documents()

    def test_check_sources_of_documents_list_diff(self):
        db_dir = pathlib.Path(__file__).parent / "build_files" / "output" / "database_doc_list"
        conf = self.create_config('test')
        conf.documents.input.path = ["docs.jsonl", "docs3.jsonl"]
        conf.rerank.input.db.path = str(db_dir)
        builder = PipelineBuilder(conf)
        with pytest.raises(ConfigError):
            builder.check_sources_of_documents()
