import logging

from .config import load_yaml_config
from .input import TrecCorpusReader
from .text import DocProcessorConfig, TextProcessor

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

from pydantic import BaseModel

class Foo(BaseModel):
    count: int
    size: float = None


class Bar(BaseModel):
    apple = 'x'
    banana = 'y'


class Spam(BaseModel):
    foo: Foo
    bars: Bar

import pprint

class Pipeline:
    def __init__(self, config_filename):
        with open(config_filename, 'r') as fp:
            config = load_yaml_config(fp)

        doc_config = config['input']['documents']
        doc_processing_config = config['document_process']

        pprint.pprint(config)

        dpc = DocProcessorConfig(**doc_processing_config)
        doc_processor = TextProcessor(dpc)

        corpus = TrecCorpusReader(doc_config['path'], doc_config['lang'], doc_config['encoding'])

    def run(self):
        print("running")
