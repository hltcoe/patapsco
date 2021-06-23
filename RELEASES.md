
## 0.9.3
 * Fixes reranker shell input so that jsonl is used
 * Adds a lang field to the top level of topic jsonl object
 * Fixes creation of sqlite database for parallel jobs
 * Using tabs to separate columns in scoring output to match trec_eval

## 0.9.2
 * Adds support in retrieval for query likelihood model and pseudo relevance feedback
 * Topics can now be filtered by lang_supported field
 * Additional validation added for catching bad data in topics and qrels

## 0.9.1
 * Symbolic links are resolved for input paths (documents, topics, qrels)

## 0.9.0
 * Conda environment includes GPU version of pytorch
 * Lucene index built with term vectors to support RM3
 * Sqlite document database uses table name 'patapsco'
 * Sqlite document database stores docs in JSON rather than pickles