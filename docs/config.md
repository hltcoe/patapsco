# Patapsco Configuration
A Patapsco run is configured with a YAML or JSON file.
The schema for the configuration is defined in [schema.py](../patapsco/schema.py).

## Structure
A configuration file contains a section for the specific run and then a section per task (documents, database, index, topics, queries, retrieve, rerank, score).
Partial runs are possible by only including a subset of the tasks.


### run
Defines properties of the run and how the pipelines work.

| field    | required | description |
| -------- | -------- | ----------- |
| name     | yes      | Name of run. Used to create output directory. |
| path     | no       | Absolute path or relative to current directory. If not specified, set to current-dir/runs/run-name. |
| parallel | no       | nested parallel configuration information |
| stage1   | no       | Stage 1 config or false |
| stage2   | no       | Stage 2 config or false |

#### stage config
| field             | required | description |
| ----------------- | -------- | ----------- |
| mode              | no       | 'streaming' or 'batch'. Default is 'streaming'. |
| batch_size        | no       | Integer size of the batch. |
| num_jobs          | no       | If parallel run, how many sub-jobs. |
| progress_interval | no       | Integer number of items to process between progress updates. |

#### parallel config
| field             | required | description |
| ----------------- | -------- | ----------- |
| name              | yes      | 'mp', 'qsub', or 'sbatch'. |
| queue             | no       | Defaults to all.q. |
| email             | no       | Your email address if desire notifications. |
| resources         | no       | qsub resources. Default is 'h_rt=12:00:00'. |
| code              | no       | additional code to insert into the bash scripts. |

The `code` parameter is useful if you need to configure the environment that your job is running in.
Examples include activating a conda environment, adding modules, or setting environment variables.
To insert multiple lines for the code parameter use a `|`:
```yaml
  code: |
    export MY_VAR=12345
    module add java
```

If using slurm, set the name to `sbatch` and set the queue to the proper partition.
In addition, the resources variable needs to be set as the default value only works with qsub.
The resources can be set to a comma separate list of resources like so:
```yaml
  resources: --time 2:00:00, --mem 6G
```

For additional qsub resources, separate them by commas with no spaces:
```yaml
  resources: h_rt=12:00:00,mem_free=8G
```

#### example
```yaml
run:
  name: HC4 Russian with param x
  parallel:
    name: mp
  stage1:
    num_jobs: 20
  stage2:
    num_jobs: 2
```

### database
The document database is for the rerankers and only needs to be created once per dataset.
The documents are normalized (control characters removed, smart quotes normalized) and stored in a database.

| field    | required | description |
| -------- | -------- | ----------- |
| name     | yes      | Name of database type (only 'sqlite' currently). |
| output   | no       | Path to database file, true for default or false to not create database. |

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

* stem: spacy, stanza, porter (for eng), parsivar (for fas)
* stopwords: lucene, baidu (for zho)
* tokenize: whitespace, spacy, stanza, moses, ngram, jieba (for zho)

The default is to use lucene for stop words, to not stem, to lowercase when normalizing text.
Tokenization must be specified.

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

The possible fields are title and desc.
To combine fields, use a plus sign: `title+desc`

```yaml
topics:
  input:
    format: json
    lang: eng
    source: original
    encoding: utf8
    path: /exp/scale21/path/to/topics
  fields: title
```

The source field is used to identify what produced the title and descriptions fields.
The HC4 data sets use "original" for the original English queries.
Experiments may also involve various translations that are described in the source field.

To filter out topics that do not have qrels for that language, use the parameter `qrels_lang`.
For example, you may want to use English topics, but only those that have judgments for Russian:

```yaml
topics:
  input:
    format: json
    lang: eng
    source: original
    qrels_lang: rus
    encoding: utf8
    path: /exp/scale21/path/to/topics
  fields: title
```

Not all options are available for all topic readers.

### queries
Prepare the queries for retrieval.
This includes text normalization and query enrichment.
Normally this section inherits from documents:

```yaml
queries:
  process:
    inherit: documents.process
```

If you have queries that you want to run directly (and skip the query creation processing in the topics task),
set the input path to point to the jsonl file of the queries:
```yaml
queries:
  input:
    path: /path/to/queries.jsonl
  process:
    inherit: documents.process
```
The jsonl format for queries is defined in `formats.md`.

If you want to do different text processing on the documents and queries, set `strict_check` under `process` to false.
This is useful if preprocessing the queries in specific ways outside of Patapsco.

#### PSQ
For PSQ queries, you need to configure both the `queries` and `retrieval` sections.
The `psq` subsection of `queries` includes any text processing to be done such as stemming.

```yaml
queries:
     process:
       inherit: documents.process
     psq:
       path: eng_table.json
       lang: eng
       stem: porter
```

The path points to a PSQ dictionary which is a json file:
```
{
  "cat": {"gato":  0.8, "felino":  0.15},
  "dog": {"pero":  0.9, "can":  0.20},
  "bird": {"pájaro": 0.6, "ave": 0.38, "galla": 0.02},
  "hello": {"hola": 0.8, "oiga":  0.000001}
}
```

And in the retrieve part of the config use psq: true:

```
retrieve:
  name: bm25
  psq: true
```

#### lucene classic query parsing
Lucene supports term weighting and boolean queries.
To use this rather than the standard queries in pyserini, turn on the `parse`
flag in both the `queries` and `retrieval` sections.

### retrieve
The most basic config for retrieve looks like this:

```yaml
retrieve:
  name: bm25
  number: 1000
```

Supported names:
 * bm25 - Okapi Best Match
 * qld - Query Likelihood with Dirichlet smoothing

bm25 parameters:
```yaml
  k1: 0.9
  b: 0.4
```

qld parameter:
```yaml
  mu: 1000
```

Query expansion with relevance model 3 (rm3):
```yaml
  rm3: true
  fb_terms: 10
  fb_docs: 10
  original_query_weight: 0.5
```

Probabilistic structured query:
```yaml
  psq: true
```
An example that uses bm25 with rm3:
```yaml
retrieve:
  name: bm25
  number: 1000
  rm3: true
```
This uses default parameters for both bm25 and rm3.

#### logging
Lucene explanations can be logged using the parameters:
```yaml
  log_explanations: true
  log_explanations_cutoff: 10
```
The cutoff controls how many of the top documents have their explanations logged.

RM3 query expansion can be logged also:
```yaml
  rm3_logging: true
```

Note that the RM3 expanded queries are not included in the Lucene explanations.

### prebuilt index
If running just stage 2 of Patapsco, you need to configure the location of the index:
```yaml
retrieve:
  input:
    index:
      path: /path/to/lucene/directory
```
This is generally the `index` directory from a previous run. 

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

### prebuilt index
If running just stage 2 of Patapsco, you need to configure the location of the document database:
```yaml
retrieve:
  input:
    database:
      path: /path/to/database/directory
```
This is generally the `database` directory from a previous run. 


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
Use `--set` for each parameter overriding:
```
--set retrieve.number=500 --set documents.process.tokenize=stanza
```

Because of a limitation with the current config code, only parameters 
in the config file can be overridden.
We plan to add support for overriding all parameters including defaults.

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

YAML does not support starting a string with a `{` character so strings that start with an interpolated value
need to be quoted like this:
```yaml
run:
  name: "{retrieval.name} for English"
```

## Validation
Most sections of the config are strictly validated.
Missing parameters, extra parameters, and incorrect types are detected.
The exception are sections that support calling external scripts with arbitrary parameters.
