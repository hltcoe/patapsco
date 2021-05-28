# Patapsco file formats
Patapsco defines a few file formats for intermediate results.
These can be used by researchers to tap into the Patapsco pipeline at various points.
For example, a query file could be generated with special syntax or enrichment data
and the stage 2 pipeline run from that file as input rather than the topics file.

## Doc file format
Patapsco makes documents available of this form:

```
{
  "id": "1",
  "lang": "eng",
  "date": "2021-02-21",
  "text": "This is a test document."
}
```

| field  | description |
| ------ | ----------- |
| id     | Document id  |
| lang   | ISO 639-3 |
| date   | YYYY-MM-DD or empty string |
| text   | Text of the document |

The `text` field will have the basic normalization run on it.
This includes removing control characters, standardizing spaces and smart quotes, and collapsing combining characters.


## Query file format
Patapsco saves and reads query files with the following jsonl format:

```json
{
  "id": "1",
  "lang": "eng",
  "query": "flooding",
  "text": "Flooding",
  "report": "A flood is an overflow of water that submerges land that is usually dry."
}
```

| field  | description |
| ------ | ----------- |
| id     | Query id  |
| lang   | ISO 639-3 |
| query  | Query syntax |
| text   | Plain text version of the query |
| report | Text of the report or empty string |

The `lang` field should be the language of the text which may not be the language of the original topic.

The `text` field is used by reranking and should not have any stemming or other destructive processing.

The `query` could be the original text with some processing (like stemming) or could include query syntax for the retrieval system.


## Results file format
Patapsco saves the retrieval results in jsonl:

```json
{
  "query": {
    "id": "1",
    "lang": "eng",
    "query": "flooding",
    "text": "Flooding",
    "report": "A flood is an overflow of water that submerges land that is usually dry."
  },
  "doc_lang": "eng",
  "system": "PyseriniRetriever",
  "results": [
    {
      "doc_id": "47f004f9-360e-40d3-90a4-8bd2ce88bf03",
      "rank": 0,
      "score": 1.628999948501587
    },
    {
      "doc_id": "eb00e10f-a076-4b24-8ffa-2da13b795ad1",
      "rank": 1,
      "score": 1.577299952507019
    }
  ]
}
```

The results object includes the query object, the language of the documents, and the system name.

It also contains a list of results where each result has a doc_id string, rank, and score.
