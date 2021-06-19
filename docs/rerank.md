# Rerank interface
Patapsco, as currently designed, does not have a train phase.
It is expected that artifacts will be produced using Patapsco like 
normalized documents, indexes, and result sets that can be used in training.
The training itself will involve specialized frameworks and code that is best
decoupled from the pipeline.

Once training is complete, the model is wrapped in a script and called from Patapsco.
This allows participants working on upstream tasks like query enrichment, translation or
retrieval to run a particular reranking model to evaluate performance for their component.

## Script interface
Patapsco can be configured to call a script for reranking.
The script is passed the path to the document database and retrieval results.
The script should rerank the results and write out new results to the specified location.

Example of calling a reranking script
```
./rerank.sh [doc_lang] [query_lang] [db_path] [input_path] [output_path]
```
These arguments are positional. 
More information on configurable optional arguments below.

| argument    | description |
| ----------- | ----------- |
| doc_lang    | ISO 639-3 |
| query_lang  | ISO 639-3 |
| db_path     | Path to sqlite3 db file |
| input_path  | Path to jsonl file with retrieval results |
| output_path | Path to trec output file to write |

If an implementation does not support the specified language pairs, use a non-zero exit code.

### Document database
The document format is described in `formats.md`.
It is built as a key-value store using sqlite3.
The path to this database is provided.
The table name is `patapsco` and the column names are `key` and `value`.
The document is stored as JSON.

### Results data
The results format is described in `formats.md`.
Each json object contains the query and the retrieved results.

### Configuration
In a Patapsco configuration file, the script path is set for reranking and any additional arguments.

```yaml
rerank:
  name: shell
  script: /path/to/my/script
  magic: 77
```

In the above example, a parameter `magic` is defined with value 77.
The script will be called with those as an argument and value pair:
```
/path/to/my/script --magic 77
```
This supports doing experiments through configuration.
Different models or processing could occur based on these parameters.
Debugging could be turned on or off.

### Debugging
Turn on output in the rerank section of the Patapsco config:
```yaml
rerank:
  name: shell
  script: /path/to/my/script
  output: true
```
This will create a rerank sub-directory under the run directory. 
In that directory will be a shell directory with a log file of everything written to stdout or stderr.
