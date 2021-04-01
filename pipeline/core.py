import collections
import json
import pathlib


Doc = collections.namedtuple('Doc', ('id', 'lang', 'text'))
Topic = collections.namedtuple('Topic', ('id', 'lang', 'title', 'desc', 'narr'))
Result = collections.namedtuple('Result', ('topic_id', 'doc_id', 'rank', 'score', 'name'))


class DocWriter:
    def __init__(self, path):
        self.dir = pathlib.Path(path)
        self.dir.mkdir(parents=True)

    def write(self, doc):
        path = self.dir / doc.id
        with open(path, 'w') as fp:
            fp.write(doc.text)


class TopicWriter:
    def __init__(self, path):
        dir = pathlib.Path(path)
        dir.mkdir(parents=True)
        path = dir / 'topics.json'
        self.file = open(path, 'w')

    def write(self, topic):
        self.file.write(json.dumps(topic._asdict()) + "\n")

    def close(self):
        self.file.close()


class ResultsWriter:
    def __init__(self, path):
        dir = pathlib.Path(path)
        dir.mkdir(parents=True)
        self.path = dir / 'results.txt'
        self.file = open(self.path, 'w')

    def write(self, results):
        for result in results:
            self.file.write(f"{result.topic_id} Q0 {result.doc_id} {result.rank} {result.score} {result.name}\n")

    def close(self):
        self.file.close()
