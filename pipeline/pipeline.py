import logging

from .config import load_yaml_config
from .core import DocWriter
from .input import DocumentReaderFactory
from .text import TextProcessor

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

        doc_config = config['input']['documents']
        self.doc_reader = DocumentReaderFactory.create(doc_config)

        doc_processing_config = config['document_process']
        self.doc_processor = TextProcessor(doc_processing_config)
        self.doc_writer = DocWriter(doc_processing_config['output'])

    def run(self):
        for doc in self.doc_reader:
            doc = self.doc_processor.run(doc)
            self.doc_writer.write(doc)
