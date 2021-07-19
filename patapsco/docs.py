import collections
import csv
import dataclasses
import gzip
import json
import logging
import pathlib
from typing import Optional

from .error import ParseError
from .pipeline import Task
from .schema import DocumentsInputConfig
from .text import TextProcessor
from .util import DataclassJSONEncoder, InputIterator, ReaderFactory
from .util.file import count_lines, count_lines_with, path_append
from .util.formats import parse_sgml_documents
from .util.normalize import compare_strings

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class Doc:
    id: str
    lang: str
    text: str
    date: Optional[str]


class DocumentReaderFactory(ReaderFactory):
    classes = {
        'sgml': 'SgmlDocumentReader',
        'json': 'Hc4JsonDocumentReader',
        'jsonl': 'Hc4JsonDocumentReader',
        'msmarco': 'TsvDocumentReader',
    }
    config_class = DocumentsInputConfig
    name = "input document type"


class SgmlDocumentReader(InputIterator):
    """Iterator that reads a TREC sgml document"""

    def __init__(self, path, encoding, lang, **kwargs):
        self.path = path
        self.encoding = encoding
        self.lang = lang
        self.docs_iter = iter(parse_sgml_documents(path, encoding))

    def __iter__(self):
        return self

    def __next__(self):
        doc = next(self.docs_iter)
        return Doc(doc[0], self.lang, doc[1], None)

    def __len__(self):
        return count_lines_with('<DOC>', self.path, self.encoding)


class Hc4JsonDocumentReader(InputIterator):
    """Read documents from a JSONL file to start a pipeline"""

    def __init__(self, path, encoding, lang, **kwargs):
        """
        Args:
            path (str): Path to file to parse
            encoding (str): Encoding of file
            lang (str): Language of documents in file
        """
        self.path = path
        self.encoding = encoding
        self.lang = lang
        open_func = gzip.open if path.endswith('.gz') else open
        self.fp = open_func(path, 'rt', encoding=encoding)
        self.count = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.fp.closed:
            raise StopIteration
        self.count += 1
        line = self.fp.readline()
        if not line:
            self.fp.close()
            raise StopIteration
        try:
            data = json.loads(line.strip())
            return Doc(data['id'], self.lang, ' '.join([data['title'].strip(), data['text'].strip()]), data['date'])
        except json.decoder.JSONDecodeError as e:
            raise ParseError(f"Problem parsing json from {self.path} on line {self.count}: {e}")
        except KeyError as e:
            raise ParseError(f"Missing field {e} in json element in {self.path} on line {self.count}")

    def __len__(self):
        return count_lines(self.path, self.encoding)


class TsvDocumentReader(InputIterator):
    """Iterator that reads TSV documents from MSMARCO Passages"""

    def __init__(self, path, encoding, lang, **kwargs):
        self.path = path
        self.encoding = encoding
        self.lang = lang
        open_func = gzip.open if path.endswith('.gz') else open
        self.fp = open_func(path, 'rt', encoding=encoding)
        self.reader = csv.reader(self.fp, delimiter='\t')

    def __iter__(self):
        return self

    def __next__(self):
        try:
            row = next(self.reader)
            return Doc(row[0], self.lang, row[1], None)
        except StopIteration:
            self.fp.close()
            raise

    def __len__(self):
        return count_lines(self.path, self.encoding)


class DocWriter(Task):
    """Write documents to a json file using internal format"""

    def __init__(self, run_path, config, artifact_config):
        super().__init__(run_path, artifact_config, config.output)
        path = self.base / 'documents.jsonl'
        self.file = open(path, 'w')

    def process(self, doc):
        """
        Args:
            doc (Doc)

        Returns:
            Doc
        """
        # if no database, we remove the extra text object before serializing
        if hasattr(doc, 'original_text'):
            del doc.original_text
        self.file.write(json.dumps(doc, cls=DataclassJSONEncoder) + "\n")
        return doc

    def end(self):
        super().end()
        self.file.close()

    def reduce(self, dirs):
        for base in dirs:
            path = path_append(base, 'documents.jsonl')
            with open(path) as fp:
                for line in fp:
                    self.file.write(line)


class DocReader(InputIterator):
    """Iterator over documents written by DocWriter"""

    def __init__(self, path):
        self.path = pathlib.Path(path)
        if self.path.is_dir():
            self.path = self.path / 'documents.jsonl'
        self.file = open(self.path, 'r')

    def __iter__(self):
        return self

    def __next__(self):
        if self.file.closed:
            raise StopIteration
        line = self.file.readline()
        if not line:
            self.file.close()
            raise StopIteration
        data = json.loads(line)
        return Doc(**data)

    def __len__(self):
        return count_lines(self.path)


class DocumentProcessor(TextProcessor):
    """Document Preprocessing"""
    MAX_TEXT_LEN = 1000000  # throw out documents longer than a million characters

    def __init__(self, run_path, config, lang):
        """
        Args:
            run_path (str): Root directory of the run.
            config (DocumentsConfig)
            lang (str): Language code for the documents.
        """
        super().__init__(run_path, config.process, lang)
        self.save_report = config.process.normalize.report
        self.diffs = collections.Counter()

    def process(self, doc):
        """
        Args:
            doc (Doc)

        Returns
            Doc
        """
        text = original_text = doc.text
        if len(text) > self.MAX_TEXT_LEN:
            LOGGER.warning(f"Rejecting {doc.id} because it exceeds the length limit with a length of {len(text)}")
            return None
        text = self.pre_normalize(text)
        doc.original_text = text  # this for the database to use
        if self.save_report:
            self.diffs += compare_strings(original_text, text)

        tokens = self.tokenize(text)
        stopword_indices = self.identify_stop_words(tokens)
        tokens = self.stem(tokens)
        tokens = self.remove_stop_words(tokens, stopword_indices)
        text = self.post_normalize(' '.join(tokens))
        doc.text = text
        return doc

    def end(self):
        if self.save_report:
            self._save_report()

    def _save_report(self):
        with open(self.run_path / 'normalize_report.txt', 'w') as fp:
            for change, count in self.diffs.most_common(len(self.diffs)):
                if "\n" not in change:  # skip newline removal
                    fp.write(f"{repr(change)}\t{count}\n")
