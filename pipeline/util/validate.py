"""Ensures system output has valid structure:
    - The submission file must contain at least 5 columns
    - The second column of the sumbission file must be "QO"
    - The fifth column must be numeric
    - No document should be listed which is not part of the qrels
    - There should be no additional query IDs and a warning will be issued if
      a query is missing
"""

import argparse as ap
import pandas as pd


def validate(args):
    try:
        df_preds = pd.read_csv(args.system_out, sep=' ', header=None)
        dfs = []
        for f in args.qrels:
            data_frame = pd.read_csv(f, sep=' ', header=None)
            dfs.append(data_frame)
        df_qrels = pd.concat(dfs)

    except pd.errors.ParserError as e:
        print(e)
        print('This file is not valid')

    else:
        preds_0_set = set(df_preds[0])
        qrels_0_set = set(df_qrels[0])

        if args.check_query_equivalence:
            if preds_0_set == qrels_0_set:
                print('Queries in output and qrels are the same')
            else:
                print('Queries in output and qrels are not the same')

        assert (len(df_preds.columns) >= 5), \
        ('The submission file must contain least 5 columns')
        assert (set(df_preds[1]) == {'Q0'}), \
            ('The second column of the ''submission must be "Q0"')
        assert (df_preds[4].dtype == float), \
            ('The fifth column must be numeric')
        assert (len(preds_0_set - qrels_0_set) == 0), \
            (f'The following queries are missing:\n'
                  f'{preds_0_set - qrels_0_set}')
        assert (len(set(df_preds[2]) - set(df_qrels[2])) == 0), \
            ('No document should be in output that is not in qrels')

        print('This file is valid')


if __name__=="__main__":
    parser = ap.ArgumentParser()
    parser.add_argument('--system_out', help='A single system output file')
    parser.add_argument('--qrels', nargs='+',
                         help='One or more qrel files from a run.')
    parser.add_argument('--check_query_equivalence', action='store_true',
                         help='Checks if output queries equal qrels queries')
    args = parser.parse_args()
    validate(args)


