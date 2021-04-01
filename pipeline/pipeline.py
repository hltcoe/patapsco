import logging

from .config import load_yaml_config
from .core import DocWriter, TopicWriter, ResultsWriter
from .input import DocumentReaderFactory, TopicReaderFactory
from .index import IndexerFactory
from .rerank import RerankFactory
from .retrieve import RetrieverFactory
from .text import DocumentProcessor, TopicProcessor

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, config_filename):
        with open(config_filename, 'r') as fp:
            config = load_yaml_config(fp)

        topic_config = config['input']['topics']
        self.topic_reader = TopicReaderFactory.create(topic_config)
        topic_process_config = config['topic_process']
        self.topic_processor = TopicProcessor(topic_process_config)
        self.topic_writer = TopicWriter(topic_process_config['output'])

        doc_config = config['input']['documents']
        self.doc_reader = DocumentReaderFactory.create(doc_config)
        doc_process_config = config['document_process']
        self.doc_processor = DocumentProcessor(doc_process_config)
        self.doc_writer = DocWriter(doc_process_config['output'])

        index_config = config['index']
        self.indexer = IndexerFactory.create(index_config)

        retrieve_config = config['retrieve']
        retrieve_config['input'] = index_config['output']
        self.retriever = RetrieverFactory.create(retrieve_config)
        self.retrieve_writer = ResultsWriter(retrieve_config['output'])

        rerank_config = config['rerank']
        self.reranker = RerankFactory.create(rerank_config)
        self.rerank_writer = ResultsWriter(rerank_config['output'])

    def run(self):
        for doc in self.doc_reader:
            doc = self.doc_processor.run(doc)
            self.doc_writer.write(doc)
            self.indexer.index(doc)
        self.indexer.close()

        results = {}
        for topic in self.topic_reader:
            topic = self.topic_processor.run(topic)
            self.topic_writer.write(topic)

            topic_results = self.retriever.retrieve(topic.id, topic.title)
            self.retrieve_writer.write(topic_results)
            results[topic.id] = topic_results

            topic_results = self.reranker.rerank(topic.id, topic.title, topic_results)
            self.rerank_writer.write(topic_results)
        self.topic_writer.close()
        self.retrieve_writer.close()
        self.rerank_writer.close()
