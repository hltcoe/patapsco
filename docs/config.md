# Patapsco Configuration
A Patapsco run is configured with a YAML or JSON file.
The schema for the configuration is defined in [schema.py](../patapsco/schema.py).

## Structure
A configuration file contains a section for the specific run and then a section per task (documents, index, topics, queries, retrieve, rerank, score).

### run
Defines properties of the run and how the pipelines work.

#### required
**name**: descriptive name of the run that is turned into the run output directory by default.

#### optional
**path**: override the default location of the run output directory.

**stage1** and **stage2**: optional configuration for the stage 1 and stage 2 pipelines.

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
    tokenize:
      name: stanza
    lowercase: true
    stem:
      name: stanza
```

**output**: optionally write the output of the text processing to disk.

```yaml
  output:
    path: processed-docs
```

to be continued...