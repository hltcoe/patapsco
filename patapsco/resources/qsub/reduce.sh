#!/bin/bash

#$ -N patapsco-reduce-stage-{stage}
#$ -j y
#$ -o {base}
#$ -l {resources}
{email}

DATE=$(date '+%Y-%m-%d %H:%M:%S,%3N')
PYTHON_VERSION=$(python --version)
PYTHON_EXE=$(which python)
echo "$DATE - patapsco-reduce - INFO - $PYTHON_VERSION"
echo "$DATE - patapsco-reduce - INFO - $PYTHON_EXE"

patapsco-reduce {debug} --stage {stage} {config}
