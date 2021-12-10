# Patapsco commands
Patapsco installs a few commands beyond the primary pipeline runner: `patapsco`.

## Normalization
A document can be normalized using Patapsco's text normalization code.

```commandline
patapsco-norm doc.txt > new_doc.txt
```
This performs character normalization, but not tokenization or stemming.

## Querying
A Lucene index built from a run can be queried.

```commandline
patapsco-query --index path/to/index --query "this is my query"
```
There are a large number of arguments available. See the help output.

## Web Services
Patapsco includes JSON web services for retrieving documents and querying the index.

```commandline
patapsco-web --run path/to/run --port 9090
```
The web services have two endpoints:
 * documents: `/doc/<id>`
 * querying: `/query/<query>`
 
All configuration is read from the config.yml file in the run directory.
