#!/bin/bash

#$ -N patapsco
#$ -j y
#$ -o {base}
#$ -l h_rt=12:00:00
#$ -t 1-{num_jobs}

JOB_ID = $(($SGE_TASK_ID-1))
echo $JOB_ID

patapsco-grid {debug} --stage {stage} --job $JOB_ID --increment {increment} {config}
