import collections
import csv
import gzip
import xml.etree.ElementTree as ElementTree

import bs4

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


def parse_sgml_topics(path, xml_prefix=None, encoding='utf8'):
    """Parse from SGML"""
    if xml_prefix is None:
        xml_prefix = ''
    title_tag = xml_prefix + 'title'
    desc_tag = xml_prefix + 'desc'
    narr_tag = xml_prefix + 'narr'

    with open(path, 'r', encoding=encoding) as fp:
        text = fp.read()
    text = "<topics>\n" + text + "\n</topics>"
    root = ElementTree.fromstring(text)
    for topic in root:
        num = topic.find('num').text.strip()
        title = topic.find(title_tag).text.strip()
        desc = topic.find(desc_tag).text.strip()
        narr = topic.find(narr_tag).text.strip()
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
            qrels[row[0]][row[2]] = int(row[3])
    yield qrels
