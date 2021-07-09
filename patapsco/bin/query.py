import argparse
import json
import pathlib
import tempfile

from patapsco.retrieve import PyseriniRetriever, RetrieveConfig
from patapsco.schema import PathConfig, RetrieveInputConfig
from patapsco.topics import Query
from patapsco.util.file import delete_dir


# TODO - PSQ, other langs
def main():
    parser = argparse.ArgumentParser(description="Query a lucene index.")
    parser.add_argument("-i", "--index", required=True, help="Path to lucene index")
    parser.add_argument("-q", "--query", required=True,  help="Query string")
    args = parser.parse_args()

    temp_dir = pathlib.Path(tempfile.mkdtemp()) 
    conf = RetrieveConfig(name="bm25", input=RetrieveInputConfig(index=PathConfig(path=args.index)))
    query = Query(id="123", lang="eng", query=args.query, text="", report=None)
    pr = PyseriniRetriever(run_path=temp_dir, config=conf)
    results = pr.process(query)
    for result in results.results:
        print(json.dumps({result.doc_id: (result.rank, result.score)}))
    delete_dir(temp_dir)

if __name__ == '__main__':
    main()
