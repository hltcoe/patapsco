# preprocessing step for moses

import argparse
from pathlib import Path
import shutil

from util import pbar

from multiprocessing import Pool
from functools import partial

from patapsco.text import TokenizerStemmerFactory, TextProcessor

lang_map = {
    'ar': 'ara',
    'en': 'eng',
    'fa': 'fas',
    'ru': 'rus',
    'zh': 'zho',
}

class RawBitextProcessor(TextProcessor):
    
    def __init__(self, config):
        super().__init__(None, config, lang_map[config.lang]) # omit the path
    
    def process(self, text):
        text = text.replace('\t', ' ')
        text = self.pre_normalize(text)
        tokens = self.tokenize(text)
        stopword_indices = self.identify_stop_words(tokens)
        tokens = self.stem(tokens)
        tokens = self.remove_stop_words(tokens, stopword_indices)

        return self.post_normalize(' '.join(tokens))

def get_flags(args):
    return f"{args.tokenize}-{args.stem if args.stem else 'nostem'}" \
           f"{'-lower' if args.lowercase else ''}" \
           f"{'-rmsw' if args.stopwords else ''}"

def worker(args, info):
    wid, temp_file, raw_text = info
    # wid, temp_file, start, end = info

    processor = RawBitextProcessor(args)
    processor.begin()
    main_proc = (wid < 1)
    
    # fast forward
    if args.resume and temp_file.exists() and \
        sum(1 for _ in temp_file.open()) == len(raw_text):
        print(f"Skipping task {wid}.")
        return 

    with temp_file.open('a' if wid == -1 else 'w') as fw:
        for line in pbar(raw_text, desc='Worker 0', disable=not(main_proc)):
            fw.write(processor.process(line) + "\n")



def main(args):
    args.normalize = argparse.Namespace(lowercase=args.lowercase)
    args.stopwords = 'lucene' if args.rm_stopwords else False 
    args.model_path = None

    if not args.raw_text.exists():
        raise FileNotFoundError(f"Raw text file {args.raw_text} does not exists.")
    
    args.output_path.mkdir(exist_ok=True, parents=True)
    output_file = args.output_path / f"{args.raw_text.stem}.tok.{get_flags(args)}.{args.lang}"

    if output_file.exists():
        raise FileExistsError(f"Output file {output_file} exists. The run have already finished.")

    temp_file = output_file.parent / f"{output_file.name}.writing"

    all_raw_text = [ l.strip() for l in pbar(args.raw_text.open(newline='\n'), desc='reading text') ]
    end = len(all_raw_text)
    print(f"Total {end} lines")

    worker_ = partial(worker, args)
    if args.nworker == 1:
        worker_((-1, temp_file, all_raw_text))
    else:
        # sanity check for number of workers when resuming
        existing_temps = list(temp_file.parent.glob(f"{temp_file.name}.*"))
        if args.resume and len(existing_temps) > 0:
            assert len(existing_temps) == args.nworker, \
                   "Number of workers does not align with the temp files being resumed. " \
                   "This will create misalignment of the bitext. "

        line_per_worker = end // args.nworker + 1
        with Pool(args.nworker) as pool:
            pool.map(worker_, [
                (wid, temp_file.parent / f"{temp_file.name}.{wid}", 
                 all_raw_text[ wid*line_per_worker: (wid+1)*line_per_worker ])
                for wid in range(args.nworker)
            ])
        # combining 
        with temp_file.open('a') as fw:
            for wid in pbar(range(args.nworker), desc='Combining files'):
                for rl in pbar((temp_file.parent / f"{temp_file.name}.{wid}").open(), total=line_per_worker):
                    fw.write(rl)
                (temp_file.parent / f"{temp_file.name}.{wid}").unlink()

    shutil.copy( temp_file, output_file)
    temp_file.unlink()
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser("Script for processing the bitext")
    parser.add_argument('--raw_text', type=Path, required=True, help="Raw text file, one line per sentence.")
    parser.add_argument('--lang', choices=['en', 'zh', 'fa', 'ru'], required=True, help="Language of the text.")

    # Compatible with Patapsco
    parser.add_argument('--lowercase', action='store_true', default=False, help="Do case-folding.")
    parser.add_argument('--rm_stopwords', action='store_true', default=False, help="Remove stopwords. ")
    parser.add_argument('--tokenize', choices=TokenizerStemmerFactory.tokenizers, required=True, help="Tokenizer")
    parser.add_argument('--stem', choices=TokenizerStemmerFactory.stemmers, default=None, 
                        help="Stemmer to use, default is no stemming.")
    
    parser.add_argument('--output_path', type=Path, required=True, help="Output path for the processed text.")

    parser.add_argument('--resume', action='store_true', default=False, 
                        help="Resume from existing runs if possible. This automatically trust the content in the "
                             "existing temp files. Use with caution.")
    parser.add_argument('--nworker', type=int, default=1, help="Number of multiprocessing workers.")

    main(parser.parse_args())