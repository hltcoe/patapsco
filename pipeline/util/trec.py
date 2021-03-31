# From OpenNIR MIT License
import gzip
import re

import bs4

DOC_TEXT_TAGS = ["<TEXT>", "<HEADLINE>", "<TITLE>", "<HL>", "<HEAD>", "<TTL>", "<DD>", "<DATE>", "<LP>", "<LEADPARA>"]
DOC_TEXT_END_TAGS = ["</TEXT>", "</HEADLINE>", "</TITLE>", "</HL>", "</HEAD>", "</TTL>", "</DD>", "</DATE>", "</LP>", "</LEADPARA>"]


def parse_sgml(path, encoding='utf8'):
    docs = []
    if path.endswith('.gz'):
        open_fn = gzip.open
    else:
        open_fn = open
    with open_fn(path, 'rt', encoding=encoding, errors='replace') as file:
        docid = None
        doc_text = ''
        tag_no = None
        while file:
            line = next(file, StopIteration)
            if line is StopIteration:
                break
            if line.startswith('<DOC ') or line.startswith('<DOC>'):
                match = re.match(r".*id=\"([^\"]+)\".*", line)
                if match:
                    docid = match.group(1)
            elif line.startswith('<DOCNO>'):
                while '</DOCNO>' not in line:
                    l = next(file, StopIteration)
                    if l is StopIteration:
                        break
                    line += l
                docid = line.replace('<DOCNO>', '').replace('</DOCNO>', '').strip()
            elif line.startswith('</DOC>'):
                assert docid is not None
                docs.append((docid, bs4.BeautifulSoup(doc_text, "html.parser").get_text()))
                docid = None
                doc_text = ''
                tag_no = None
            elif tag_no is not None:
                doc_text += line
                if line.startswith(DOC_TEXT_END_TAGS[tag_no]):
                    tag_no = None
            else:
                for i, tag in enumerate(DOC_TEXT_TAGS):
                    if line.startswith(tag):
                        tag_no = i
                        doc_text += line
                        break
    return docs
