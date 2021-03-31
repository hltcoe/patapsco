import logging

from .config import load_yaml_config
from .core import DocWriter, TopicWriter
from .input import DocumentReaderFactory, TopicReaderFactory
from .index import IndexerFactory
from .text import DocumentProcessor, TopicProcessor

logger = logging.getLogger(__name__)


class Module:
    def __init__(self, config):
        pass





class DocumentProcess(Module):
    pass


class QueryProcess(Module):
    pass


class Index(Module):
    pass


class Retrieve(Module):
    pass


class Rerank(Module):
    pass


class Score(Module):
    pass


class Splitter(Module):
    def __init__(self, *modules):
        self.modules = modules


class Combiner(Module):
    pass


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

    def run(self):
        for doc in self.doc_reader:
            doc = self.doc_processor.run(doc)
            self.doc_writer.write(doc)
            self.indexer.index(doc)

        for topic in self.topic_reader:
            topic = self.topic_processor.run(topic)
            self.topic_writer.write(topic)

        self.topic_writer.close()
        self.indexer.close()
