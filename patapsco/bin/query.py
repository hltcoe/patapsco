import argparse
import pathlib

from patapsco.retrieve import PyseriniRetriever, RetrieveConfig
from patapsco.schema import PathConfig, PSQConfig, QueriesConfig, RetrieveInputConfig, TextProcessorConfig
from patapsco.text import TextProcessor
from patapsco.topics import Query, QueryProcessor


def main():
    parser = argparse.ArgumentParser(description="Query a lucene index.")
    parser.add_argument("-i", "--index", required=True, help="Path to lucene index")
    parser.add_argument("-q", "--query", required=True, help="Query string")

    parser.add_argument("--query_lang", default="eng", choices=["eng", "fas", "rus", "zho"], help="Language to query in")
    parser.add_argument("--stem", default=False, choices=["spacy", "stanza", "porter"], help="If set, stem query")
    parser.add_argument("--stopwords", default=False, choices=["lucene", "baidu"], help="If set, remove stopwords")
    parser.add_argument("-c", "--count", type=int, help="How many results to return")

    query_syntax = parser.add_mutually_exclusive_group()
    query_syntax.add_argument("--bool", action="store_true", help="If set, expect boolean query string")
    query_syntax.add_argument("--psq", help="Path to PSQ json dictionary")

    bm25_group = parser.add_argument_group("bm25 parameters")
    bm25_group.add_argument("--b", type=float, default=0.9, help="b parameter")
    bm25_group.add_argument("--k1", type=float, default=0.4, help="k1 parameter")

    qld_group = parser.add_argument_group("qld parameters")
    qld_group.add_argument("--qld", action="store_true", help="If set, retrieval uses QLD")
    qld_group.add_argument("--mu", type=int, default=1000, help="mu parameter")

    rm3_group = parser.add_argument_group("rm3 parameters")
    rm3_group.add_argument("--rm3", action="store_true", help="If set, retrieval uses  rm3 query expansion")
    rm3_group.add_argument("--fb_terms", type=int, default=10, help="fb terms parameter")
    rm3_group.add_argument("--fb_docs", type=int, default=10, help="fb docs parameter")
    rm3_group.add_argument("--original_query_weight", type=float, default=0.5, help="original query weight parameter")

    args = parser.parse_args()

    name = "psq" if args.psq else "qld" if args.qld else "bool" if args.bool else "bm25"
    parse = True if args.bool else False

    text_config = TextProcessorConfig(tokenize="whitespace", stopwords=args.stopwords, stem=args.stem)
    processor = TextProcessor(run_path="", config=text_config, lang=args.query_lang)
    processor.begin()

    lang_path = pathlib.Path(args.index) / '.lang'
    doc_lang = lang_path.read_text().strip()
    psq = PSQConfig(path=args.psq, lang=doc_lang) if args.psq else None
    queries = QueriesConfig(process=text_config, psq=psq, parse=parse)
    qp = QueryProcessor(run_path="", config=queries, lang=args.query_lang)
    qp.begin()
    query = Query("1", lang=args.query_lang, query="", text=args.query, report="")
    proc = qp.process(query)

    conf = RetrieveConfig(name=name, input=RetrieveInputConfig(index=PathConfig(path=args.index)), parse=parse, k1=args.k1, b=args.b, mu=args.mu, rm3=args.rm3,
                          fb_terms=args.fb_terms, fb_docs=args.fb_docs, original_query_weight=args.original_query_weight)
    pr = PyseriniRetriever(run_path="", config=conf)
    pr.begin()
    results = pr.process(proc)
    if results.results:
        for i, result in enumerate(results.results):
            if i == args.count:
                break
            print(f"{result.doc_id}\t{result.score}")
    else:
        print("No results")


if __name__ == "__main__":
    main()
