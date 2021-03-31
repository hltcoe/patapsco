from .core import Doc
from .util.trec import parse_sgml


class TrecCorpusReader:
    def __init__(self, path, lang, encoding='utf8'):
        self.lang = lang
        self.docs = parse_sgml(path, encoding)

    def __iter__(self):
        return self

    def __next__(self):
        doc = next(self.docs)
        return Doc(doc[0], self.lang, doc[1])
