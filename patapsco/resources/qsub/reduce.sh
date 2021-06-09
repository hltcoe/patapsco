#!/bin/bash

#$ -N patapsco-reduce-stage-{stage}
#$ -j y
#$ -o {base}
#$ -l {resources}
{email}

patapsco-reduce {debug} --stage {stage} {config}
