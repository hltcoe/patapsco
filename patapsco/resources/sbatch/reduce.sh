#!/bin/bash

#SBATCH --job-name=patapsco-reduce-stage-{stage}
#SBATCH -o {base}/patapsco-reduce-stage-{stage}-%j.out
{resources}
{email}

{code}

DATE=$(date '+%Y-%m-%d %H:%M:%S,%3N')
PYTHON_VERSION=$(python --version)
PYTHON_EXE=$(which python)
echo "$DATE - patapsco-reduce - INFO - $PYTHON_VERSION"
echo "$DATE - patapsco-reduce - INFO - $PYTHON_EXE"

if [[ -n "$CUDA_VISIBLE_DEVICES" ]]; then
  echo "$DATE - patapsco-reduce - INFO - Using gpus $CUDA_VISIBLE_DEVICES"
fi

patapsco-reduce {debug} --stage {stage} {config}
