# Patapsco - the SCALE 2021 Pipeline

## Requirements
Patapsco requires Python 3.6+

## Install
It is best to create a virtual environment using Python's venv module or conda.
After creating and activating the environment, install dependencies:
```
pip install -r requirements.txt
```

## Configuration

## Running

## Submitting Results

## Development
To install the development dependencies, run:
```
pip install -r dev_requirements.txt
```

### Unit Tests
To run the unit tests, run:
```
pytest
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
