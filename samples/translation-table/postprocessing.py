import argparse
import json
from collections import defaultdict
from pathlib import Path
import pandas as pd

from util import pbar

def work(s, prob_cutoff, cdf_cutoff, max_translation, no_normalize):
    s = s.sort_values('prob')[::-1]
    s = s[s.prob >= prob_cutoff]
    s = s[s.prob.cumsum() / s.prob.sum() <= cdf_cutoff]
    if max_translation > 0:
        s = s[:max_translation]
    if not no_normalize:
        s = s.assign(prob=s.prob / s.prob.sum())
    return s

def main(args):
    if args.output_file.exists():
        if not args.overwrite:
            raise FileExistsError(args.output_file)
        print(f"Will overwrite output file `{args.output_file}`.")

    table = defaultdict(dict)
    for line in pbar(args.lex_file.open(), desc='loading'):
        line = line.strip().split(" ")
        table[line[0]][line[1]] = float(line[2])
    
    for eng in pbar(table, desc='trimming'):
        s = pd.Series(table[eng]).sort_values()[::-1]
        if args.prob_cutoff > 0.:
            s = s[s >= args.prob_cutoff]
        if args.cdf_cutoff < 1.0:
            s = s[s.cumsum() / s.sum() <= args.cdf_cutoff]
        if args.max_translation > 0:
            s = s[:args.max_translation]
        if not args.no_normalize:
            s = s / s.sum()
        table[eng] = s.to_dict()

    print(f"Writing table to file `{args.output_file}`...")
    json.dump(table, args.output_file.open('w'))


if __name__ == '__main__':
    parser = argparse.ArgumentParser("Script for converting the a lex file from giza++ to a PSQ dictionary.")
    parser.add_argument('lex_file', type=Path, help="The lex.e2f file from giza++ script.")
    parser.add_argument('output_file', type=Path, help="File name for the output PSQ dictionary.")

    parser.add_argument('--max_translation', type=int, default=-1, 
                        help="Maximum number of alternative translations. Default is without limit.")
    parser.add_argument('--cdf_cutoff', type=float, default=1.0, 
                        help="Cutoff of the cumulative density function. Default is no cutoff.")
    parser.add_argument('--prob_cutoff', type=float, default=0.0, 
                        help="Minimum probability for alternative translation. Default is 0.")
    parser.add_argument('--no_normalize', action='store_true', default=False, 
                        help="Without marginalizing the translation probabilities.")

    parser.add_argument('--overwrite', action='store_true', default=False, help="Overwrite the existing table.")

    main( parser.parse_args() )