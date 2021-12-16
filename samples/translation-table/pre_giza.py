import argparse 
from pathlib import Path

from util import pbar

def main(args):
    args.output_dir.mkdir(exist_ok=True, parents=True)
    with (args.output_dir / f"tok{args.filea.suffix}").open('w') as fwa, \
         (args.output_dir / f"tok{args.fileb.suffix}").open('w') as fwb:
        for linea, lineb in pbar(zip(args.filea.open(), args.fileb.open()), total=args.n_lines):
            if linea.strip() != "" and lineb.strip() != "":
                fwa.write(linea)
                fwb.write(lineb)

if __name__ == '__main__':
    parser = argparse.ArgumentParser("Script for removing pair of processed text that is empty in either language. "
                                     "Example: `python pre_giza.py tok.en tok.zh 20000 ./temp/`")
    parser.add_argument('filea', type=Path, help='File A of bitext with language code as the last suffix.')
    parser.add_argument('fileb', type=Path, help='File B of bitext with language code as the last suffix.')
    parser.add_argument('--n_lines', type=int, default=None, help='Number of lines.')
    parser.add_argument('--output_dir', type=Path, required=True,
                        help="Directory to output the pair of files with file names `tok.lang_a` and `tok.lang_b`.")
    
    main(parser.parse_args())