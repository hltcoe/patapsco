import collections
import csv
import gzip
import xml.etree.ElementTree as ElementTree

import bs4


def parse_documents(path, encoding='utf8'):
    DOC_TEXT_TAGS = ["headline", "title", "hl", "head", "ttl", "dd", "date", "lp", "leadpara", "text"]
    open_func = gzip.open if path.endswith('.gz') else open
    with open_func(path, 'rt', encoding=encoding, errors='replace') as fp:
        soup = bs4.BeautifulSoup(fp, 'html.parser')
        for doc in soup.find_all('doc'):
            doc_id = doc.docno.get_text()
            text_parts = []
            for tag in DOC_TEXT_TAGS:
                obj = doc.find(tag)
                if obj:
                    text_parts.append(obj.get_text().strip())
            yield doc_id, ' '.join(text_parts)


def parse_topics(path, xml_prefix=None, encoding='utf8'):
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


def parse_qrels(path):
    with open(path, 'r') as fp:
        reader = csv.reader(fp, delimiter=' ')
        qrels = collections.defaultdict(dict)
        for row in reader:
            qrels[row[0]][row[2]] = int(row[3])
    yield qrels
