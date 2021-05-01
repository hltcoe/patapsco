import pathlib
import tempfile

import pytest

from patapsco.index import MockIndexer
from patapsco.job import *
from patapsco.schema import *


class TestJobBuilder:
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
            run=RunConfig(name='run name', path=str(output_directory)),
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
        conf = RunnerConfig(run=RunConfig(name='test'))
        builder = JobBuilder(conf)
        with pytest.raises(ConfigError, match='No tasks are configured to run'):
            builder.build()

    def test_create_plan1_with_normal_config(self):
        conf = self.create_config('test')
        builder = JobBuilder(conf)
        stage1_plan = builder._create_stage1_plan()
        assert Tasks.DOCUMENTS in stage1_plan
        assert Tasks.INDEX in stage1_plan

    def test_create_plan2_with_normal_config(self):
        conf = self.create_config('test')
        builder = JobBuilder(conf)
        stage2_plan = builder._create_stage2_plan()
        assert Tasks.TOPICS in stage2_plan
        assert Tasks.QUERIES in stage2_plan
        assert Tasks.RETRIEVE in stage2_plan
        assert Tasks.RERANK in stage2_plan
        assert Tasks.SCORE in stage2_plan

    def test_create_plan_with_complete_documents(self):
        conf = self.create_config('docs_complete')
        builder = JobBuilder(conf)
        stage1_plan = builder._create_stage1_plan()
        assert Tasks.DOCUMENTS not in stage1_plan
        assert Tasks.INDEX in stage1_plan

    def test_create_plan_with_incomplete_documents(self):
        conf = self.create_config('docs_incomplete')
        builder = JobBuilder(conf)
        stage1_plan = builder._create_stage1_plan()
        assert Tasks.DOCUMENTS in stage1_plan
        assert Tasks.INDEX in stage1_plan

    def test_create_plan_with_complete_documents_and_complete_index(self):
        conf = self.create_config('stage1_complete')
        builder = JobBuilder(conf)
        stage1_plan = builder._create_stage1_plan()
        assert stage1_plan == []

    def test_create_plan_with_incomplete_documents_and_complete_index(self):
        conf = self.create_config('docs_incomplete_index_complete')
        builder = JobBuilder(conf)
        stage1_plan = builder._create_stage1_plan()
        assert stage1_plan == []

    def test_create_plan_with_complete_topics(self):
        conf = self.create_config('topics_complete')
        builder = JobBuilder(conf)
        stage2_plan = builder._create_stage2_plan()
        assert Tasks.TOPICS not in stage2_plan
        assert Tasks.QUERIES in stage2_plan

    def test_create_plan_with_complete_queries(self):
        conf = self.create_config('queries_complete')
        builder = JobBuilder(conf)
        stage2_plan = builder._create_stage2_plan()
        assert Tasks.TOPICS not in stage2_plan
        assert Tasks.QUERIES not in stage2_plan
        assert Tasks.RETRIEVE in stage2_plan

    def test_create_plan_with_complete_retrieval(self):
        conf = self.create_config('retrieval_complete')
        builder = JobBuilder(conf)
        stage2_plan = builder._create_stage2_plan()
        assert Tasks.TOPICS not in stage2_plan
        assert Tasks.QUERIES not in stage2_plan
        assert Tasks.RETRIEVE not in stage2_plan
        assert Tasks.RERANK in stage2_plan

    def test_create_plan_with_complete_rerank(self):
        conf = self.create_config('rerank_complete')
        builder = JobBuilder(conf)
        with pytest.raises(ConfigError, match="Rerank is already complete. Delete its output directory to rerun reranking."):
            builder._create_stage2_plan()

    def test_create_plan_with_no_rerank_retrieval(self):
        conf = self.create_config('test')
        conf.rerank = None
        conf.retrieve = None
        builder = JobBuilder(conf)
        with pytest.raises(ConfigError, match="Scorer can only run if either retrieve or rerank is configured."):
            builder._create_stage2_plan()

    def test_build_stage1_with_standard_docs(self):
        conf = self.create_config('test')
        builder = JobBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        tasks = builder._get_stage1_tasks(plan)
        assert len(tasks) == 2
        assert isinstance(tasks[0], DocumentProcessor)
        assert isinstance(tasks[1], DocWriter)

    def test_build_stage1_with_standard_docs_but_no_output(self):
        conf = self.create_config('test')
        conf.documents.output = False
        builder = JobBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        tasks = builder._get_stage1_tasks(plan)
        assert len(tasks) == 1
        assert isinstance(tasks[0], DocumentProcessor)

    def test_build_stage1_with_bad_doc_format(self):
        conf = self.create_config('test')
        conf.documents.input.format = "abc"
        builder = JobBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        with pytest.raises(ConfigError, match="Unknown input document type: abc"):
            builder._get_stage1_iterator(plan)

    def test_build_stage1_with_bad_doc_encoding(self):
        conf = self.create_config('test')
        conf.documents.input.encoding = "abc"
        builder = JobBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        with pytest.raises(ConfigError, match="abc is not a valid file encoding"):
            builder._get_stage1_iterator(plan)

    def test_build_stage1_with_no_files(self):
        conf = self.create_config('test')
        conf.documents.input.path = "nothing"
        builder = JobBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        with pytest.raises(ConfigError, match="No files match pattern"):
            builder._get_stage1_iterator(plan)

    def test_build_stage1_with_bad_lang(self):
        conf = self.create_config('test')
        conf.documents.input.lang = "da"
        builder = JobBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        with pytest.raises(ConfigError, match="Unknown language code: da"):
            builder._get_stage1_tasks(plan)

    def test_build_stage1_with_bad_tokenizer(self):
        conf = self.create_config('test')
        conf.documents.process.tokenize.name = "nothing"
        builder = JobBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        with pytest.raises(ConfigError):
            builder._get_stage1_tasks(plan)

    def test_build_stage1_with_standard_docs_split(self):
        conf = self.create_config('test')
        conf.documents.process.splits = ['tokenize', 'tokenize+lowercase']
        builder = JobBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        tasks = builder._get_stage1_tasks(plan)
        assert len(tasks) == 2
        assert isinstance(tasks[0], DocumentProcessor)
        assert tasks[0].splitter.splits == {'tokenize': 'tokenize', 'lowercase': 'tokenize+lowercase'}
        assert isinstance(tasks[1], MultiplexTask)

    def test_build_stage1_with_standard_docs_and_bad_split(self):
        conf = self.create_config('test')
        conf.documents.process.splits = ['tokenize', 'tokenize+uppercase']
        builder = JobBuilder(conf)
        plan = [Tasks.DOCUMENTS]
        with pytest.raises(ConfigError, match="Unrecognized split"):
            builder._get_stage1_tasks(plan)

    def test_build_stage1_with_no_documents_for_indexer(self):
        conf = self.create_config('test')
        conf.documents = None
        builder = JobBuilder(conf)
        plan = [Tasks.INDEX]
        with pytest.raises(ConfigError, match="index not configured with documents"):
            builder._get_stage1_iterator(plan)

    def test_build_stage1_with_indexer_gets_docs_from_documents(self):
        conf = self.create_config('docs_complete')
        builder = JobBuilder(conf)
        plan = [Tasks.INDEX]
        builder._get_stage1_iterator(plan)
        tasks = builder._get_stage1_tasks(plan)
        assert isinstance(tasks[0], MockIndexer)

    def test_build_stage1_with_indexer_gets_docs_from_input(self):
        conf = self.create_config('docs_complete')
        conf.documents = None
        conf.index.input = IndexInputConfig(documents=PathConfig(path=str(self.dir / "build_files" / "output" / "docs_complete" / "docs")))
        builder = JobBuilder(conf)
        plan = [Tasks.INDEX]
        builder._get_stage1_iterator(plan)
        tasks = builder._get_stage1_tasks(plan)
        assert isinstance(tasks[0], MockIndexer)

    def test_build_stage1_with_indexer_gets_bad_docs_from_input(self):
        conf = self.create_config('test')
        conf.documents = None
        conf.index.input = IndexInputConfig(documents=PathConfig(path=str(self.dir / "build_files" / "output" / "docs_incomplete" / "docs")))
        builder = JobBuilder(conf)
        plan = [Tasks.INDEX]
        with pytest.raises(ConfigError, match="Unable to load artifact config"):
            builder._get_stage1_iterator(plan)

    def test_build_stage1_with_standard_docs_index(self):
        conf = self.create_config('test')
        builder = JobBuilder(conf)
        plan = [Tasks.DOCUMENTS, Tasks.INDEX]
        tasks = builder._get_stage1_tasks(plan)
        assert len(tasks) == 3
        assert isinstance(tasks[0], DocumentProcessor)
        assert isinstance(tasks[1], DocWriter)
        assert isinstance(tasks[2], MockIndexer)

    def test_build_stage1_with_bad_pipeline_mode(self):
        conf = self.create_config('test')
        conf.run.stage1.mode = "fast"
        builder = JobBuilder(conf)
        plan = [Tasks.DOCUMENTS, Tasks.INDEX]
        iterator = builder._get_stage1_iterator(plan)
        tasks = builder._get_stage1_tasks(plan)
        with pytest.raises(ConfigError, match="Unrecognized pipeline mode"):
            builder._build_stage1_pipeline(iterator, tasks)

    def test_build_stage2_with_standard_topics(self):
        conf = self.create_config('test')
        builder = JobBuilder(conf)
        plan = [Tasks.TOPICS]
        tasks = builder._get_stage2_tasks(plan)
        assert len(tasks) == 2
        assert isinstance(tasks[0], TopicProcessor)
        assert isinstance(tasks[1], QueryWriter)

    def test_build_stage2_with_standard_topics_but_no_output(self):
        conf = self.create_config('test')
        conf.topics.output = False
        builder = JobBuilder(conf)
        plan = [Tasks.TOPICS]
        tasks = builder._get_stage2_tasks(plan)
        assert len(tasks) == 1
        assert isinstance(tasks[0], TopicProcessor)

    def test_build_stage2_with_bad_topic_format(self):
        conf = self.create_config('test')
        conf.topics.input.format = "abc"
        builder = JobBuilder(conf)
        plan = [Tasks.TOPICS]
        with pytest.raises(ConfigError, match="Unknown topic type: abc"):
            builder._get_stage2_iterator(plan)

    def test_build_stage2_with_bad_encoding(self):
        conf = self.create_config('test')
        conf.topics.input.encoding = "abc"
        builder = JobBuilder(conf)
        plan = [Tasks.TOPICS]
        with pytest.raises(ConfigError, match="abc is not a valid file encoding"):
            builder._get_stage2_iterator(plan)

    def test_build_stage2_with_no_files(self):
        conf = self.create_config('test')
        conf.topics.input.path = "nothing"
        builder = JobBuilder(conf)
        plan = [Tasks.TOPICS]
        with pytest.raises(ConfigError, match="No files match pattern"):
            builder._get_stage2_iterator(plan)

    def test_build_stage2_with_bad_lang(self):
        conf = self.create_config('test')
        conf.topics.input.lang = "da"
        builder = JobBuilder(conf)
        plan = [Tasks.TOPICS]
        with pytest.raises(ConfigError, match="Unknown language code: da"):
            builder._get_stage2_tasks(plan)

    def test_build_stage2_queries_with_no_topics(self):
        conf = self.create_config('test')
        conf.topics = None
        builder = JobBuilder(conf)
        plan = [Tasks.QUERIES]
        with pytest.raises(ConfigError, match="query processor not configured with input"):
            builder._get_stage2_iterator(plan)

    def test_build_stage2_queries_with_topic_output(self):
        conf = self.create_config('topics_complete')
        builder = JobBuilder(conf)
        plan = [Tasks.QUERIES]
        builder._get_stage2_iterator(plan)
        tasks = builder._get_stage2_tasks(plan)
        assert isinstance(tasks[0], QueryProcessor)

    def test_build_stage2_queries_with_query_input(self):
        conf = self.create_config('topics_complete')
        conf.topics = None
        conf.queries.input = QueriesInputConfig(path=str(self.dir / "build_files" / "output" / "topics_complete" / "topics"))
        builder = JobBuilder(conf)
        plan = [Tasks.QUERIES]
        builder._get_stage2_iterator(plan)
        tasks = builder._get_stage2_tasks(plan)
        assert isinstance(tasks[0], QueryProcessor)

    def test_build_stage2_queries_with_query_input_to_file(self):
        conf = self.create_config('topics_complete')
        conf.topics = None
        conf.queries.input = QueriesInputConfig(path=str(self.dir / "build_files" / "output" / "topics_complete" / "topics" / "queries.jsonl"))
        builder = JobBuilder(conf)
        plan = [Tasks.QUERIES]
        builder._get_stage2_iterator(plan)
        tasks = builder._get_stage2_tasks(plan)
        assert isinstance(tasks[0], QueryProcessor)
        assert tasks[0].lang == "en"

    def test_check_sources_of_documents_standard_config(self):
        conf = self.create_config('test')
        builder = JobBuilder(conf)
        builder.check_sources_of_documents()

    def test_check_sources_of_documents_str_same(self):
        db_dir = pathlib.Path(__file__).parent / "build_files" / "output" / "database"
        conf = self.create_config('test')
        conf.documents.input.path = "docs.jsonl"
        conf.rerank.input.db.path = str(db_dir)
        builder = JobBuilder(conf)
        builder.check_sources_of_documents()

    def test_check_sources_of_documents_str_diff(self):
        db_dir = pathlib.Path(__file__).parent / "build_files" / "output" / "database"
        conf = self.create_config('test')
        conf.documents.input.path = "docs_other_data.jsonl"
        conf.rerank.input.db.path = str(db_dir)
        builder = JobBuilder(conf)
        with pytest.raises(ConfigError):
            builder.check_sources_of_documents()

    def test_check_sources_of_documents_type_diff(self):
        db_dir = pathlib.Path(__file__).parent / "build_files" / "output" / "database"
        conf = self.create_config('test')
        conf.documents.input.path = ["docs_other_data.jsonl", "more_docs.jsonl"]
        conf.rerank.input.db.path = str(db_dir)
        builder = JobBuilder(conf)
        with pytest.raises(ConfigError):
            builder.check_sources_of_documents()

    def test_check_sources_of_documents_list_same(self):
        db_dir = pathlib.Path(__file__).parent / "build_files" / "output" / "database_doc_list"
        conf = self.create_config('test')
        conf.documents.input.path = ["docs.jsonl", "docs2.jsonl"]
        conf.rerank.input.db.path = str(db_dir)
        builder = JobBuilder(conf)
        builder.check_sources_of_documents()

    def test_check_sources_of_documents_list_diff(self):
        db_dir = pathlib.Path(__file__).parent / "build_files" / "output" / "database_doc_list"
        conf = self.create_config('test')
        conf.documents.input.path = ["docs.jsonl", "docs3.jsonl"]
        conf.rerank.input.db.path = str(db_dir)
        builder = JobBuilder(conf)
        with pytest.raises(ConfigError):
            builder.check_sources_of_documents()

    def test_check_text_processing(self):
        conf = self.create_config('test')
        builder = JobBuilder(conf)
        builder.check_text_processing()

    def test_check_text_processing_mistmatch(self):
        conf = self.create_config('test')
        conf.queries.process.lowercase = False
        builder = JobBuilder(conf)
        with pytest.raises(ConfigError):
            builder.check_text_processing()
