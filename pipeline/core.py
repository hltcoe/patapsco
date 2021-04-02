import collections
import pathlib


Result = collections.namedtuple('Result', ('query_id', 'doc_id', 'rank', 'score', 'name'))


class DocWriter:
    def __init__(self, path):
        self.dir = pathlib.Path(path)
        self.dir.mkdir(parents=True)

    def write(self, doc):
        path = self.dir / doc.id
        with open(path, 'w') as fp:
            fp.write(doc.text)


class ResultsWriter:
    def __init__(self, path):
        dir = pathlib.Path(path)
        dir.mkdir(parents=True)
        self.path = dir / 'results.txt'
        self.file = open(self.path, 'w')

    def write(self, results):
        for result in results:
            self.file.write(f"{result.query_id} Q0 {result.doc_id} {result.rank} {result.score} {result.name}\n")

    def close(self):
        self.file.close()


class ResultsAccumulator:
    def __init__(self):
        self.run = collections.defaultdict(dict)

    def add(self, results):
        for result in results:
            self.run[result.query_id][result.doc_id] = result.score
