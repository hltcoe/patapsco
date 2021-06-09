#!/bin/bash

#$ -N patapsco-map
#$ -j y
#$ -o {base}
#$ -l h_rt=12:00:00
#$ -t 1-{num_jobs}

# we want zero-based job ids
JOB_ID=$(($SGE_TASK_ID-1))

patapsco-map {debug} --stage {stage} --job $JOB_ID --increment {increment} {config}
