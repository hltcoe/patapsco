# Patapsco - the SCALE 2021 Pipeline

## Requirements
Patapsco requires Python 3.6+ and Java 11+

To check your Java version:
```
javac --version
```

## Install

### Create a Python virtual environment using venv or conda.

#### With Python's venv module
Create and activate the virtual environment:
```
python3 -m venv venv
source venv/bin/activate
```
Install Patapsco and its dependencies:
```
pip install --editable .
```

#### With conda
Create and activate the conda environment:
```
conda env create --file environment.yml
conda activate scale2021
```
Install Patapsco:
```
pip install .
```

## Design
Patapsco consists of two pipelines:
  - Stage 1: creates an index from the documents
  - Stage 2: retrieves results for queries from the indexes and reranks the results

A pipeline consists of a sequence of tasks.
  - Stage 1 tasks: 
    - text processing of documents (character normalization, tokenization, etc.)
    - indexing
  - Stage 2 tasks: 
    - extract query from topic
    - text processing of query (same as document processing)
    - retrieval of results
    - reranking of results
    - scoring

When a run is complete, its output is written to a run directory.
Tasks also store artifacts in the run directory that can be used for other runs.
For example, an index created in one run can be used in another.

Patapsco can run partial pipelines.
For example, a user can run just stage 1 to generate an index.
Or a user may run only stage 2 and have it start with processed queries.

## Configuration
Patapsco uses YAML or JSON files for configuration.
The stage 1 and stage 2 pipelines are built from the configuration.
The output including any artifacts (like processed queries or an index) are stored in a run directory.
For more information on configuration, see `docs/config.md`.

## Running
After installing Patapsco, a sample run is started with:
```
patapsco samples/configs/en_basic.yml
```

## Submitting Results
A run's output file plus the configuration used to generate the run can be submitted at the website: 
https://scale21.org (not ready yet)

## Development
Developers should install Patapsco in editable mode along with development dependencies:
```
pip install -e .[dev]
```

### Unit Tests
To run the unit tests, run:
```
pytest
```

Some tests load models and are normally skipped. To run those:
```
pytest --runslow
```

### Code Style
The code should conform to the PEP8 style except for leniency on line length.

To update the code, you can use autopep8.
To run it on a file:
```
autopep8 -i [path to file]
```

To test PEP8 compliance, run:
```
flake8 patapsco
```
