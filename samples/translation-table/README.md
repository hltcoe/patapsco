# Creating Translation Tables for PSQ

This readme provides a quick instruction and scripts for creating translation tables for running PSQ. 
This process involves runnining [Moses decoder](https://github.com/moses-smt/mosesdecoder). 
Please refer to their [GiHub repository](https://github.com/moses-smt/mosesdecoder) 
and [Website](https://www.statmt.org/moses/) for installation and full 
documentation.

## Preprocess bibtext

We support preprocessing the bitext through Patapsco text processing APIs. 
The following commands help you preprocess your pair of bitext in the configurations 
that will align with the query and document processing when running retrieval. 

The bitext should come in a pair of text files with one file contains only one language of the bitext. 
The number of lines in each file should be identical. 

The full documentation of the arguments can be found in `python preprocess.py --help`.

```bash
python preprocess.py \
--raw_text ./bitext/zh-en/train.raw.en \
--lang zh --lowercase --tokenize moses --rm_stopwords \
--output_path ./preprocessed_bitext/zh-en/ \
--nworker 4

python preprocess.py \
--raw_text ./bitext/zh-en/train.raw.zh \
--lang zh --tokenize ngram --rm_stopwords \
--output_path ./preprocessed_bitext/zh-en/ \
--nworker 4
```

## Run GIZA++

Since GIZA++ requires bitext that has no empty lines, the following command helps you remove the pair of sentences 
that becomes empty in either or both languages. 
Since the preprocessing configuration of each language could be different in each language, e.g. removing stopwords in 
one but retain them in another, it is important to clean up the preprocessed bitext before running GIZA++.

```bash
python pre_giza.py \
./preprocessed_bitext/zh-en/train.raw.tok.moses-nostem-lower-rmsw.en \
./preprocessed_bitext/zh-en/train.raw.tok.ngram-nostem-rmsw.zh \
--output_dir ./cleaned/
```

The following command will train the translation model via Moses. 
Instructions for installing it can be found at https://github.com/moses-smt/mosesdecoder. 
The path to moses and [external binaries](https://www.statmt.org/moses/?n=Moses.Baseline) need to be included in the environment variables. 
The arguments of the script are 
`[number process] [memory size] [souce language] [target language] [source bitext file] [target bitext file] [output directory]`. 
This process could take days depending on the computational resource. 

Many thanks to [Kevin Duh](https://www.cs.jhu.edu/~kevinduh/) for contributing this script. 

```bash
MOSES={path/to/moses/} EXT_BIN={path/to/ext_binaries/for/mgiza} \
./run_giza.sh 32 10G zh en ./cleaned/tok.zh ./cleaned/tok.en ./giza_out/
```

## Postprocess to create translation table

Finally, we post process the resulting lexical translation file into PSQ translation table. 
Since GIZA++ provide all possible translations for each token based on cooccurance, trimming alternative translation 
is perferable. 
The following command takes in the result lexical file from GIZA++ and creates a translation table. 
The documentation of the arguments can be found in `python postprocessing.py --help`.

```bash
python postprocessing.py ./giza_out/model/lex.e2f ./table_max20-cdf0.8.dict \
--max_translation 20 --cdf_cutoff 0.8
```