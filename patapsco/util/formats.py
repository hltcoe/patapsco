import collections
import csv
import functools
import gzip
import itertools
import json
import xml.etree.ElementTree as ElementTree

import bs4
import numpy as np

from ..error import ParseError


def parse_sgml_documents(path, encoding='utf8'):
    """Parse from SGML"""
    doc_text_tags = ["headline", "title", "hl", "head", "ttl", "dd", "date", "lp", "leadpara", "text"]
    open_func = gzip.open if path.endswith('.gz') else open
    with open_func(path, 'rt', encoding=encoding) as fp:
        try:
            soup = bs4.BeautifulSoup(fp, 'html.parser')
        except UnicodeDecodeError as e:
            raise ParseError(f"Decode error for {path}: {e}")
        for doc in soup.find_all('doc'):
            doc_id = doc.docno.get_text()
            text_parts = []
            for tag in doc_text_tags:
                obj = doc.find(tag)
                if obj:
                    text_parts.append(obj.get_text().strip())
            yield doc_id, ' '.join(text_parts)


def parse_hamshahri_documents(path, encoding='utf8'):
    with open(path, 'r', encoding=encoding) as fp:
        doc_id = None
        text = []
        while True:
            line = fp.readline()
            if line == '':
                break
            line = line.strip()
            if '.DID' in line:
                if doc_id:
                    yield doc_id, ' '.join(text).strip()
                text = []
                doc_id = line.split('\t')[1]
                fp.readline()  # skip date
                fp.readline()  # skip category
            else:
                text.append(line)
        yield doc_id, ' '.join(text)


def get_sgml_field(tag):
    if tag is not None:
        return tag.text.strip()
    else:
        return None


def parse_sgml_topics(path, encoding='utf8', sgml_prefix=None):
    """Parse from SGML"""
    if not sgml_prefix:
        sgml_prefix = ''
    title_tag = sgml_prefix + 'title'
    desc_tag = sgml_prefix + 'desc'
    narr_tag = sgml_prefix + 'narr'

    with open(path, 'r', encoding=encoding) as fp:
        text = fp.read()
    text = "<topics>\n" + text + "\n</topics>"
    root = ElementTree.fromstring(text)
    for topic in root:
        num = topic.find('num').text.strip()
        title = topic.find(title_tag).text.strip()
        desc = topic.find(desc_tag).text.strip()
        narr = get_sgml_field(topic.find(narr_tag))  # narrative is optional
        yield num, title, desc, narr


def parse_xml_topics(path, encoding='utf8'):
    """Parse from XML"""
    with open(path, 'r', encoding=encoding) as fp:
        text = fp.read()
    root = ElementTree.fromstring(text)
    for topic in root:
        lang = topic.attrib['lang']
        identifier = topic.find('identifier').text.strip()
        title = topic.find('title').text.strip()
        desc = topic.find('description').text.strip()
        narr = topic.find('narrative').text.strip()
        yield identifier, lang, title, desc, narr


def parse_qrels(path):
    with open(path, 'r') as fp:
        delimiter = ' '
        first_line = fp.readline()
        if '\t' in first_line:
            delimiter = '\t'
        fp.seek(0)
        reader = csv.reader(fp, delimiter=delimiter)
        qrels = collections.defaultdict(dict)
        for row in reader:
            try:
                qrels[row[0]][row[2]] = int(row[3])
            except ValueError as e:
                raise ParseError(f"Invalid qrels format for {row}: {e}")
    yield qrels


def normalize_psq_entry(entry, cum_thresh=0.97, elem_thresh=1e-5):
    """Throw out small probabilities and normalize so sum = 1"""
    total = sum(entry.values())
    entry = {word: prob / total for word, prob in entry.items()}
    entry = {word: prob for word, prob in entry.items() if prob > elem_thresh}
    entry = dict(sorted(entry.items(), key=lambda item: item[1], reverse=True))
    if cum_thresh < 1:
        probs = np.array(list(entry.values()), dtype='float')
        cum_index = np.where(np.cumsum(probs) > cum_thresh)
        if cum_index[0].size == 0:
            index = len(entry) - 1
        else:
            index = np.where(np.cumsum(probs) > cum_thresh)[0][0]
        entry = dict(itertools.islice(entry.items(), int(index) + 1))
        total = sum(entry.values())
        entry = {word: prob / total for word, prob in entry.items()}
    return entry


def parse_psq_table(path, threshold=0.97):
    """translation table is a dictionary of dictionaries
    The inner dictionary maps target words to probabilities.
    """
    norm = functools.partial(normalize_psq_entry, cum_thresh=threshold)
    with open(path) as fp:
        trans_table = json.load(fp)
        return {k: norm(v) for k, v in trans_table.items()}
