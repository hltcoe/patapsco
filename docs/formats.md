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

## Topic file format
Patapsco reads topic files with the following jsonl format to produce query files:

```json
{
"topic_id": "1", 
"languages_with_qrels": ["zho", "fas"], 
"topics": [ {
             "lang": "eng", 
             "source": "original", 
             "topic_title": "Asteroids Endangering Earth", 
             "topic_description": "Articles related to asteroids that pose danger of impact to Earth."
            }, 
            {
             "lang": "zho", 
             "source": "human translation", 
             "topic_title": "小行星危害地球", 
             "topic_description": "与对地球构成撞击危害的小行星相关的文章。"
            }, 
            {
             "lang": "fas", 
             "source": "20220114-scale21-sockeye2-tm1", 
             "topic_title": "سیارات در معرض خطر زمین", 
             "topic_description": "مقالات مربوط به سیارک ‌ هایی که خطر برخورد با زمین را تهدید می ‌ کنند."
             }, 
             ...
          ], 
"narratives": {
                "zho": {
                         "very_valuable": "Details about asteroids ...", 
                         "somewhat_valuable": "N/A", 
                         "not_that_valuable": "Information on discussions about asteroids ...", 
                         "non_relevant": "Details about asteroids that previously ..."
                        }, 
                "fas": "Mention of asteroids...", 
              }, 
"report": {
            "url": "https://en.wikipedia.org/w/index.php?title=(415029)_2011_UL21&oldid=877055001", 
            "text": "2011 UL21 briefly had ... ", 
            "date": "2019-01-06"
          }
}
```

| field                | description                                                                           |
| -------------------- | ------------------------------------------------------------------------------------- |
| topic_id             | Topic id                                                                              |
| languages_with_qrels | list of ISO 639-3 codes                                                               |
| topics               | list of dictionaries with fields 'lang', 'source', 'topic_title', 'topic_description' |
| narratives           | dictionary where the fields are ISO 639-3 codes                                       |
| report               | dictionary where the fields are 'url', 'text', 'date'                                 |

Patapsco processes the `topics` field to determine which information should be used as the text representation of the 
`text` in the query file. The `topic` field is a list of dictionaries. Each dictionary contain the following fields:

| field             | description                                     |
| ----------------- | ----------------------------------------------- |
| lang              | ISO 639-3                                       |
| source            | A string that describes the source of the topic |
| topic_title       | Text identified as the title of the topic       |
| topic_description | Text identified as the description of the topic |

The (`lang`, `source`) must be unique for each topic in the list.

Patapsco will create the query from the `topic_title` when `fields` in the config file is `title`. The query will come from 
`topic_description` when the `fields` is `description` in the config file, and `title+description` will concatenate the two fields.

Patapsco does not read the `narratives` field; however, many IR datasets have such a field. The narratives for each language 
may be different. Therefore, the dictionary can capture that using language codes for the fields. The value of each language 
may be a string or if the narrative is structured, a dictionary. The fields of this inner dictionary are specified by the creator 
of the topics file.

The `report` field is optional. If it is present, Patapsco will add the `text` field within the report dictionary to the query file to be 
available for re-ranking processes that utilize the query file. Aside from the `text` field, any other field in the report is ignored by Patapsco.

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
