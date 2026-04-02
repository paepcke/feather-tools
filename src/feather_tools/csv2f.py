#!/usr/bin/env python
'''
Created on May 7, 2024
@author: Andreas Paepcke

Converts a CSV file to feather or parquet format.

The output format is determined in order of precedence:
  1. The extension of --dst_file if provided
  2. The --format flag if provided
  3. The name of the script itself:
       csv2f  → .feather (default)
       csv2p  → .parquet (default)

Usage:
  csv2f <csv_src_file> [--dst_file <path>] [--format {feather,parquet}] [pandas options...]
  csv2p <csv_src_file> [--dst_file <path>] [--format {feather,parquet}] [pandas options...]

Options not listed here are forwarded verbatim to pandas.read_csv().
'''

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

from feather_tools.ftools_workhorse import default_format_from_invocation, SUPPORTED_EXTS


def main(args=None, **kwargs):
    '''
    Convert a CSV file to feather or parquet format.

    The output format is inferred from (in order of priority): the
    extension of ``dst_file``, the ``--format`` flag, and the name of
    the invoked script (``csv2f`` → feather, ``csv2p`` → parquet).

    :param args: pre-parsed argparse namespace; if None, ``sys.argv`` is parsed.
    :type args: argparse.Namespace or None
    :param kwargs: used when called programmatically; must contain at least
        ``src_file``.  All other keys are forwarded to :func:`pandas.read_csv`.
    '''

    if args is None:
        invocation_default = default_format_from_invocation()

        description = (
            "Convert a .csv file to .feather or .parquet.\n"
            "Options not listed below are forwarded to pandas.read_csv().\n"
            f"Invoked as '{os.path.basename(sys.argv[0])}' → default format: {invocation_default}"
        )
        parser = argparse.ArgumentParser(
            prog=os.path.basename(sys.argv[0]),
            formatter_class=argparse.RawTextHelpFormatter,
            description=description
        )
        parser.add_argument('src_file', help='CSV file to convert')
        parser.add_argument('--dst_file',
                            default=None,
                            help='Output path. Extension overrides --format.\n'
                                 'Default: src path with extension replaced.')
        parser.add_argument('--format',
                            dest='fmt',
                            choices=['feather', 'parquet'],
                            default=invocation_default,
                            help='Output format (default: inferred from script name)')

        # pandas.read_csv pass-through options
        parser.add_argument('--delimiter', default=None, help='see pandas.read_csv')
        parser.add_argument('--header', default='infer', help='see pandas.read_csv')
        parser.add_argument('--names', default='_NoDefault.no_default', help='see pandas.read_csv')
        parser.add_argument('--index_col', default=None, help='see pandas.read_csv')
        parser.add_argument('--usecols', default=None, help='see pandas.read_csv')
        parser.add_argument('--dtype', default=None, help='see pandas.read_csv')
        parser.add_argument('--engine', default=None, help='see pandas.read_csv')
        parser.add_argument('--converters', default=None, help='see pandas.read_csv')
        parser.add_argument('--true_values', default=None, help='see pandas.read_csv')
        parser.add_argument('--false_values', default=None, help='see pandas.read_csv')
        parser.add_argument('--skipinitialspace', default=False, help='see pandas.read_csv')
        parser.add_argument('--skiprows', default=None, help='see pandas.read_csv')
        parser.add_argument('--skipfooter', default=0, help='see pandas.read_csv')
        parser.add_argument('--nrows', default=None, help='see pandas.read_csv')
        parser.add_argument('--na_values', default=None, help='see pandas.read_csv')
        parser.add_argument('--keep_default_na', default=True, help='see pandas.read_csv')
        parser.add_argument('--na_filter', default=True, help='see pandas.read_csv')
        parser.add_argument('--verbose', default='_NoDefault.no_default', help='see pandas.read_csv')
        parser.add_argument('--skip_blank_lines', default=True, help='see pandas.read_csv')
        parser.add_argument('--parse_dates', default=None, help='see pandas.read_csv')
        parser.add_argument('--date_format', default=None, help='see pandas.read_csv')
        parser.add_argument('--dayfirst', default=False, help='see pandas.read_csv')
        parser.add_argument('--cache_dates', default=True, help='see pandas.read_csv')
        parser.add_argument('--chunksize', default=None, help='see pandas.read_csv')
        parser.add_argument('--compression', default='infer', help='see pandas.read_csv')
        parser.add_argument('--thousands', default=None, help='see pandas.read_csv')
        parser.add_argument('--decimal', default='.', help='see pandas.read_csv')
        parser.add_argument('--lineterminator', default=None, help='see pandas.read_csv')
        parser.add_argument('--quotechar', default='"', help='see pandas.read_csv')
        parser.add_argument('--quoting', default=0, help='see pandas.read_csv')
        parser.add_argument('--doublequote', default=True, help='see pandas.read_csv')
        parser.add_argument('--escapechar', default=None, help='see pandas.read_csv')
        parser.add_argument('--comment', default=None, help='see pandas.read_csv')
        parser.add_argument('--encoding', default=None, help='see pandas.read_csv')
        parser.add_argument('--encoding_errors', default='strict', help='see pandas.read_csv')
        parser.add_argument('--dialect', default=None, help='see pandas.read_csv')
        parser.add_argument('--on_bad_lines', default='error', help='see pandas.read_csv')
        parser.add_argument('--low_memory', default=True, help='see pandas.read_csv')
        parser.add_argument('--memory_map', default=False, help='see pandas.read_csv')
        parser.add_argument('--float_precision', default=None, help='see pandas.read_csv')
        parser.add_argument('--storage_options', default=None, help='see pandas.read_csv')
        parser.add_argument('--dtype_backend', default='_NoDefault.no_default', help='see pandas.read_csv')

        args = parser.parse_args()

        if not os.path.exists(args.src_file):
            print(f"File {args.src_file} not found")
            sys.exit(1)

        kwargs = args.__dict__

    src_file = kwargs.pop('src_file')
    fmt      = kwargs.pop('fmt', default_format_from_invocation())

    try:
        dst_file = kwargs.pop('dst_file')
    except KeyError:
        dst_file = None

    # Determine destination path and format
    if dst_file is None:
        ext = '.feather' if fmt == 'feather' else '.parquet'
        dst_file = Path(src_file).with_suffix(ext)
    else:
        # Let the dst_file extension override the fmt flag
        dst_ext = Path(dst_file).suffix.lower()
        if dst_ext in {'.feather'}:
            fmt = 'feather'
        elif dst_ext in {'.parquet', '.pq'}:
            fmt = 'parquet'
        # else: trust --format / invocation default

    # Strip _NoDefault sentinel values — pandas uses its own internal sentinel
    csv_kwargs = {k: v for k, v in kwargs.items() if v != '_NoDefault.no_default'}

    df = pd.read_csv(src_file, **csv_kwargs)

    if fmt == 'feather':
        df.to_feather(dst_file)
    else:
        df.to_parquet(dst_file, index=False)

    print(f"Written: {dst_file}")


if __name__ == '__main__':
    main()
