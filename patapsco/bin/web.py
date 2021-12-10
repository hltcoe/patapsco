import argparse
import logging
import pathlib

import flask

from patapsco import ConfigHelper, DocumentDatabase, Query, QueryProcessor, RetrieverFactory


def main():
    logger = logging.getLogger('patapsco')
    logger.setLevel(logging.WARNING)
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logger.addHandler(console)

    parser = argparse.ArgumentParser(description="Run web services over a lucene index.")
    parser.add_argument("--run", required=True, help="Path to a run directory")
    parser.add_argument("--port", required=True, type=int, help="Port for web services")
    args = parser.parse_args()

    run_dir = pathlib.Path(args.run).absolute()
    config_path = run_dir / "config.yml"
    conf = ConfigHelper.load(str(config_path))

    db = DocumentDatabase(str(run_dir), conf.database.output, True)
    lang = conf.topics.input.lang
    query_processor = QueryProcessor(str(run_dir), conf.queries, lang)
    query_processor.begin()
    retriever = RetrieverFactory.create(str(run_dir), conf.retrieve)
    retriever.begin()

    app = flask.Flask("Patapsco web services")

    @app.route('/doc/<id>')
    def document(id):
        if id not in db:
            flask.abort(404)
        return flask.jsonify(db[id])

    @app.route('/query/<query>')
    def retrieve(query):
        query = Query(id='web', lang=lang, query=query, text=query, report=None)
        query = query_processor.process(query)
        return flask.jsonify(retriever.process(query))

    app.run('0.0.0.0', args.port)


if __name__ == "__main__":
    main()
