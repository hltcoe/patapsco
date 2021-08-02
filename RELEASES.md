## 0.9.7
 * Change scoring to penalize a query in qrels without any results
 * Limit scoring to top 1000 results
 * Add qld support for PSQ
 * Add patapsco-query command line tool for querying lucene index built with patapsco
 * Fixed a bug with handling new lines in text normalization
 * Collect memory and timing stats into log file for qsub
 * Add additional escaping in PSQ query handling
 * Don't write out scoring config to artifact configs
 * Maintain separate full config and run config when using artifacts from previous runs

## 0.9.6
 * Adds Lucene query parsing
 * Fixes bug with batch mode when running partial pipelines
 * Fixes bug with batch size larger than input iterable

## 0.9.5
 * Adds PSQ
 * Increases Java heap size
 * Better support for using symbolic links as output directories
 * Adds a character size limit for documents (1 million)
 * Adds a rule based Farsi stemmer (parsivar)
 * Collect warnings from qsub log files into base directory of run
 * Updates to pyserini 0.13.0 which has fixed RM3

## 0.9.4
 * Adds better logging of queries (query expansion and explanations)
 * Handling recursion error in porter stemmer caused by bad MT decode
 * Fixed arguments to reranker script so that ints and floats are accepted
 * Not storing raw documents in Lucene to save space and memory
 * Added flag to turn off text processing checks to support PSQ
 * Added support for slurm
 * Added code parameter to run.parallel for adding code to bash scripts
 * Upped Java heap size to prevent out of memory errors due to large documents
 * Fixed issue with using symbolic links as output directories
 * Update stanza to 1.2.1 to fix a tokenization bug

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