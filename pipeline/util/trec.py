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
                    text_parts.append(obj.get_text())
            yield doc_id, ' '.join(text_parts)


def parse_topics(filename, xml_prefix=None, encoding='utf8'):
    if xml_prefix is None:
        xml_prefix = ''
    TITLE_TAG = xml_prefix + 'title'
    DESC_TAG = xml_prefix + 'desc'
    NARR_TAG = xml_prefix + 'narr'

    with open(filename, 'r', encoding=encoding) as fp:
        text = fp.read()
    text = "<topics>\n" + text + "\n</topics>"
    root = ElementTree.fromstring(text)
    for topic in root:
        num = topic.find('num').text.strip()
        title = topic.find(TITLE_TAG).text.strip()
        desc = topic.find(DESC_TAG).text.strip()
        narr = topic.find(NARR_TAG).text.strip()
        yield num, title, desc, narr
