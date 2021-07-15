import argparse
import json
import pathlib
import tempfile

from patapsco.retrieve import PyseriniRetriever, RetrieveConfig
from patapsco.schema import PathConfig, PSQConfig, QueriesConfig, RetrieveInputConfig, TextProcessorConfig
from patapsco.text import PorterStemmer, TextProcessor
from patapsco.topics import Query, LuceneQueryGenerator, PSQGenerator, QueryGenerator, QueryProcessor
from patapsco.util.file import delete_dir


def main():
    parser = argparse.ArgumentParser(description="Query a lucene index.")
    parser.add_argument("-i", "--index", required=True, help="Path to lucene index")
    parser.add_argument("-q", "--query", required=True, help="Query string")
    query_syntax = parser.add_mutually_exclusive_group()
    query_syntax.add_argument("--bool", action="store_true", help="If set, expect boolean query string")
    query_syntax.add_argument("--psq", action="store_true", help="If set, expect PSQ query string")
    parser.add_argument("--psq_path", help="Path to PSQ dictionary")
    parser.add_argument("--qld", action="store_true", help="If set, retrieval uses QLD")
    parser.add_argument("--rm3", action="store_true", help="If set, use rm3 query expansion")
    parser.add_argument("-s", "--stem", default=False, choices=["spacy", "stanza", "porter"], help="If set, stem query using Porter stemmer")
    parser.add_argument("--stopwords", default=False, choices=["lucene", "baidu"], help="If set, remove stopwords")
    parser.add_argument("-c", "--count", type=int, help="How many results to return")
    parser.add_argument("--b", type=float, default=0.9, help="BM25 b parameter")
    parser.add_argument("--k1", type=float, default=0.4, help="BM25 k1 parameter")
    parser.add_argument("--mu", type=int, default=1000, help="QLD mu parameter")
    parser.add_argument("--fb_terms", type=int, default=10, help="RM3 fb terms parameter")
    parser.add_argument("--fb_docs", type=int, default=10, help="RM3 fb docs parameter")
    parser.add_argument("--original_query_weight", type=float, default=0.5, help="RM3 original query weight parameter")
    args = parser.parse_args()

    temp_dir = pathlib.Path(tempfile.mkdtemp())
    name = "psq" if args.psq else "qld" if args.qld else "bool" if args.bool else "bm25"

    parse = True if args.bool else False

    text_config = TextProcessorConfig(tokenize="whitespace", stopwords=args.stopwords, stem=args.stem)
    processor = TextProcessor('', text_config, "eng")
    processor.begin()

    psq = PSQConfig(path="psq.json", lang="eng") if args.psq else None
    queries = QueriesConfig(process=text_config, psq=psq, parse=parse)

    qp = QueryProcessor('', queries, "eng")
    qp.begin()
    query = Query('1', '', '', args.query, '')
    proc = qp.process(query)
    conf = RetrieveConfig(name=name, input=RetrieveInputConfig(index=PathConfig(path=args.index)), parse=parse, k1=args.k1, b=args.b, mu=args.mu, rm3=args.rm3,
                          fb_terms=args.fb_terms, fb_docs=args.fb_docs, original_query_weight=args.original_query_weight)

    pr = PyseriniRetriever(run_path='', config=conf)
    pr.begin()
    results = pr.process(proc)
    for i, result in enumerate(results.results):
        if i == args.count:
            break
        print(json.dumps({result.doc_id: (result.rank, result.score)}))
    delete_dir(temp_dir)


if __name__ == '__main__':
    main()
