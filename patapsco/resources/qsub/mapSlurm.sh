#!/bin/bash

#SBATCH --job-name=patapsco-map-stage-{stage}
#SBATCH {resources}
#SBATCH --array=1-{num_jobs}

# we want zero-based job ids
JOB_ID=$(($SLURM_ARRAY_TASK_ID-1))

DATE=$(date '+%Y-%m-%d %H:%M:%S,%3N')
PYTHON_VERSION=$(python --version)
PYTHON_EXE=$(which python)
echo "$DATE - patapsco-map - INFO - $PYTHON_VERSION"
echo "$DATE - patapsco-map - INFO - $PYTHON_EXE"

if [[ -n "$CUDA_VISIBLE_DEVICES" ]]; then
  echo "$DATE - patapsco-map - INFO - Using gpus $CUDA_VISIBLE_DEVICES"
fi

patapsco-map {debug} --stage {stage} --job $JOB_ID --increment {increment} {config}
