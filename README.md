# Patapsco - the SCALE 2021 Pipeline

## Requirements
Patapsco requires Python 3.6+ and Java 11+.

Installing Patapsco with Anaconda will add Java into the virtual environment.
If not using Anaconda, you will need to check your Java version or
enable the java module on the grid.

To check your Java version:
```
javac --version
```

On the grid, enable Java with:
```
module add java
```

## Install

### Create a Python virtual environment using venv or conda.

#### With Python's venv module
Create and activate the virtual environment:
```
python3 -m venv venv
source venv/bin/activate
```
You may need to upgrade your pip:
```
pip install -U pip
pip install -U wheel
```
Install Patapsco and its dependencies:
```
pip install --editable .
```

#### With conda
Create and activate the conda environment:
```
conda env create --file environment.yml
conda activate patapsco
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
Or a user may run only stage 2 and have it start with processed queries and a prebuilt index.

## Configuration
Patapsco uses YAML or JSON files for configuration.
The stage 1 and stage 2 pipelines are built from the configuration.
The output including any artifacts (like processed queries or an index) are stored in a run directory.
For more information on configuration, see `docs/config.md`.

## Running
After installing Patapsco, a sample run is started with:
```
patapsco samples/configs/eng_basic.yml
```

By default, the output for the run is written to a `runs` directory in the working directory.
If a run is complete, Patapsco will not overwrite it.

To turn on more detailed logging and full exception stack traces, use the debug flag:
```
patapsco --debug samples/configs/eng_basic.yml
```

Any variable in the configuration can be overriden on the command line:
```
patapsco --set run.name=my_test_run samples/configs/eng_basic.yml
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
