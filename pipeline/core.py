import collections
import pathlib


Doc = collections.namedtuple('Doc', ('id', 'lang', 'text'))


class DocWriter:
    def __init__(self, path):
        self.dir = pathlib.Path(path)
        self.dir.mkdir(parents=True)

    def write(self, doc):
        path = self.dir / doc.id
        with open(path, 'w') as fp:
            fp.write(doc.text)
