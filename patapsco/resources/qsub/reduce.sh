#!/bin/bash

#$ -N patapsco-reduce-stage-{stage}
#$ -j y
#$ -o {base}
#$ -l h_rt=12:00:00
{email}

patapsco-reduce {debug} --stage {stage} {config}
