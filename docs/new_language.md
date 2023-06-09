# Directions to add a new language

 - Place a stoplist for the language in resources (patapsco/resources/stopwords)
     - look https://github.com/apache/lucene/tree/main/lucene/analysis/common/src/resources/org/apache/lucene/analysis for lucene stoplist. 
      - If it exists, add to the resources/lucene directory. 
      - Otherwise create a directory in resources that indicates the source of the stoplist and put the list of words in a file called {lang code}.txt 
    - When setting up the config file, you need to set the stopwords attribute when you process to the documents/query to the name of the directory. By default lucene is used.
 - edit util/normalize.py to have language code and create class
 - pip install .
 - (optionally) do a pull request for the language

