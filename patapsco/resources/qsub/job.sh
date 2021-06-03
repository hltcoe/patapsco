#!/bin/bash

#$ -N patapsco
#$ -j y
#$ -o {base}
#$ -l h_rt=12:00:00

patapsco-grid {debug} --stage {stage} --increment {increment} {config}
