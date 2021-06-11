# Patapsco Configuration
A Patapsco run is configured with a YAML or JSON file.
The schema for the configuration is defined in [schema.py](../patapsco/schema.py).

## Structure
A configuration file contains a section for the specific run and then a section per task (documents, index, topics, queries, retrieve, rerank, score).
Partial runs are possible by only including a subset of the tasks.


### run
Defines properties of the run and how the pipelines work.

| field    | required | description |
| -------- | -------- | ----------- |
| name     | yes      | Name of run. Used to create output directory. |
| path     | no       | Absolute path or relative to current directory. If not specified, set to current-dir/runs/run-name. |
| parallel | no       | 'qsub' or 'mp' for parallel runs |
| stage1   | no       | Stage 1 config or false |
| stage2   | no       | Stage 2 config or false |

#### stage config
| field             | required | description |
| ----------------- | -------- | ----------- |
| mode              | no       | 'streaming' or 'batch'. Default is 'streaming'. |
| batch_size        | no       | Integer size of the batch. |
| num_jobs          | no       | If parallel run, how many sub-jobs. |
| progress_interval | no       | Integer number of items to process between progress updates. |

#### example
```yaml
run:
  name: HC4 Russian with param x
  parallel: mp
  stage1:
    num_jobs: 20
  stage2:
    num_jobs: 2
```

### database
The documents are normalized (control characters removed, smart quotes normalized) and stored in a database.

| field    | required | description |
| -------- | -------- | ----------- |
| output   | yes      | Path to database file or false to not create database. |

If running the entire pipeline, this can usually be left out of the configuration.
By default, a `database` directory is created under the output directory.

### documents
Defines properties of the document task including input, text processing, and output.

**input**: describes format and location of the input collection.

```yaml
  input:
    format: jsonl
    lang: en
    encoding: utf8
    path: /exp/scale21/some_path
```

**process**: defines the text processing of the documents including 
script normalization, tokenization, lowercasing, stopword removal, and stemming/lemmatization.

```yaml
  process:
    normalize:
      lowercase: true
    tokenize: spacy
    stem: spacy
    stopwords: lucene
```

stem: spacy, stanza, porter (for eng)
stopwords: lucene, baidu (for zho)
tokenize: whitespace, spacy, stanza, moses, ngram, jieba (for zho)

### index
The name of the indexing method.
Currently, only "lucene" is supported.

```yaml
index:
  name: lucene
```

### topics
Turn topics into queries.
Includes the input definition and what fields to select.

The possible fields are title, desc, narr.
To combine fields, use a plus sign: `title+desc`

```yaml
topics:
  input:
    format: json
    lang: eng
    encoding: utf8
    path: /exp/scale21/path/to/topics
  fields: title
```

### queries
Prepare the queries for retrieval.
This includes text normalization and query enrichment.
Normally this section inherits from documents:

```yaml
queries:
  process:
    inherit: documents.process
```

### retrieve
The only retrieve component currently is lucene with bm25 (k=1.2, b=0.75).

```yaml
retrieve:
  name: bm25
  number: 1000
```

### rerank
Rerank will usually call an external script.
The path to the script and parameters for it are configured here.
The parameters are passed as `--key value` to the script.

```yaml
rerank:
  name: shell
  script: /path/to/my/rerank.sh
  embedding: my_favorite_embedding
  secret: 42
```

### score
If there are available qrels, config them here.

```yaml
score:
  input:
    path: /exp/scale21/path/to/qrels
```

To run particular metrics, specify them in the metrics array:

```yaml
score:
  input:
    path: /exp/scale21/path/to/qrels
  metrics:
    - P_20
    - ndcg
```

## Command line overrides
Use the `--set` flag to override parameters in the configuration file.
It excepts key, value pairs separated by a comma:
```
--set retrieve.number=500,documents.process.tokenize=stanza
```

## Comments
A comment field can be added to any top level section.
It will be preserved in the config file written to the run directory.

```yaml
run:
  name: HC4 Russian with param x
  comment: In this experiment, I am transmogrifying the inner particles ...
```

## Imports
Partial configs can be imported:

```yaml
run:
  name: English test run

imports:
  - docs.yml
  - index.yml
```
Variables in the imported config will take precedence over those in the importer.
The path to the imported config files is relative to the current config file.
Nested imports are supported.

## Interpolation
Parameters in one part of the configuration ca be used elsewhere:

```yml
run:
  name: English {documents.process.stem}

documents:
  process:
    tokenize: whitespace
    stem: porter
```

## Validation
Most sections of the config are strictly validated.
Missing parameters, extra parameters, and incorrect types are detected.
The exception are sections that support calling external scripts with arbitrary parameters.