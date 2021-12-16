#!/bin/bash

# Quick script for running GIZA++ word alignments
# Many thanks to Kevin Duh for creating the original script.

if [[ $# -ne 7 ]] ; then
    echo "Quick script for running GIZA++ word alignments"
    echo "Usage: run_giza.sh [nproc] [mem_buff] [src] [trg] [bitext.src] [bitext.trg] [outdir]"
    echo " e.g. ./run_giza.sh 32 20G zh en /exp/scale18/mt/data/zh-en/ted/ted.dev.tok.zh /exp/scale18/mt/data/zh-en/ted/ted.dev.tok.en ~/tmp_giza"
    exit 1
fi

if [ -z $MOSES ]; then
    echo "Need to set moses path in variable MOSES"
    echo "Please refer to https://github.com/moses-smt/mosesdecoder for more information."
    exit 1
fi

if [ -z $EXT_BIN ]; then
    echo "Need to set path to external binaries for moses in variable EXT_BIN for -extenral-bin-dir running mgiza"
    echo "Please refer to https://www.statmt.org/moses/?n=Moses.Baseline for more information."
    exit 1
fi

nproc=$1
mem_buff=$2
src=$3
trg=$4
src_text=$5
trg_text=$6
outdir=$7

echo "Training model based on: "
echo $src_text ">>" $trg_text
echo 

tmpdir=$(mktemp -d)
ln -s $(realpath $src_text) $tmpdir/tok.$src
ln -s $(realpath $trg_text) $tmpdir/tok.$trg

bitext=$tmpdir/tok

mkdir -p $outdir/model

common="-mgiza -mgiza-cpus $nproc -sort-buffer-size $mem_buff -sort-compress gzip -cores $nproc -dont-zip -external-bin-dir $EXT_BIN -alignment grow-diag-final-and -max-phrase-length 5 -reordering hier-mslr-bidirectional-fe -f $src -e $trg "

# 1. prepare
$MOSES/scripts/training/train-model.perl $common -score-options ' --GoodTuring --CountBinFeature 1 2 3 4 6 10 --MinScore 2:0.0001' -first-step 1 -last-step 1 -corpus-dir $outdir/prepared -corpus $bitext

# 2. giza forward direction
$MOSES/scripts/training/train-model.perl $common -score-options ' --GoodTuring --CountBinFeature 1 2 3 4 6 10 --MinScore 2:0.0001' -first-step 2 -last-step 2 -corpus-dir $outdir/prepared -giza-e2f $outdir/giza -direction 2

# 2. giza inverse direction
$MOSES/scripts/training/train-model.perl $common -score-options ' --GoodTuring --CountBinFeature 1 2 3 4 6 10 --MinScore 2:0.0001' -first-step 2 -last-step 2 -corpus-dir $outdir/prepared -giza-f2e $outdir/giza-inverse -direction 1

# 3. merge alignments
$MOSES/scripts/training/train-model.perl $common -score-options ' --GoodTuring --CountBinFeature 1 2 3 4 6 10 --MinScore 2:0.0001' -first-step 3 -last-step 3 -giza-e2f $outdir/giza -giza-f2e $outdir/giza-inverse -alignment-file $outdir/model/aligned

# 4. lexical table
$MOSES/scripts/training/train-model.perl $common -score-options ' --GoodTuring --CountBinFeature 1 2 3 4 6 10 --MinScore 2:0.0001' -first-step 4 -last-step 4 -alignment-file $outdir/model/aligned -corpus $bitext -lexical-file $outdir/model/lex

# 5. extract phrases
$MOSES/scripts/training/train-model.perl $common -score-options ' --GoodTuring --CountBinFeature 1 2 3 4 6 10 --MinScore 2:0.0001' -first-step 5 -last-step 5 -alignment-file $outdir/model/aligned -extract-file $outdir/model/extract -corpus $bitext -extract-options ' --IncludeSentenceId '

# 6. build full translation table
$MOSES/scripts/training/train-model.perl $common -score-options ' --GoodTuring --CountBinFeature 1 2 3 4 6 10 --MinScore 2:0.0001' -first-step 6 -last-step 6 -alignment-file $outdir/model/aligned -extract-file $outdir/model/extract -lexical-file $outdir/model/lex -phrase-translation-table $outdir/model/phrase-table

echo "Lexical translations: $outdir/model/lex.{e2f,f2e}"

rm -rf $tmpdir
