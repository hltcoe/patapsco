#!/bin/bash

#$ -N patapsco-reduce
#$ -j y
#$ -o {base}
#$ -l h_rt=12:00:00

patapsco-reduce {debug} --stage {stage} {config}
