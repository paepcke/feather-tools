#!/usr/bin/env python
'''
Created on May 7, 2024
@author: Andreas Paepcke

Writes the dataframe contained in a .feather or .parquet file to
a .csv file (or stdout).

The input format is inferred automatically from the file extension.

Usage:
  f2csv <feather-or-parquet-file> [--dst_file <path>] [pandas options...]
  p2csv <feather-or-parquet-file> [--dst_file <path>] [pandas options...]

Both entry points are identical in behaviour; the name exists only for
user convenience / discoverability.
'''

import argparse
import os
import sys
from pathlib import Path
from io import TextIOWrapper

from feather_tools.ftools_workhorse import FToolsWorkhorse


def str2bool(bool_str):
    '''
    Convert a string representation of a boolean to an actual bool.

    :param bool_str: a string such as ``'yes'``, ``'true'``, ``'1'``, etc.,
        or an actual bool value.
    :return: the corresponding boolean
    :rtype: bool
    :raises argparse.ArgumentTypeError: if the string cannot be converted
    '''
    if isinstance(bool_str, bool):
        return bool_str
    if bool_str.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif bool_str.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError(f'Boolean value expected; encountered {bool_str}')


def main(args=None, **kwargs):
    '''
    Load a feather or parquet file and write its contents as CSV.

    The source format is inferred from the file extension; both
    ``f2csv`` and ``p2csv`` entry points call this function.

    :param args: pre-parsed argparse namespace; if None, ``sys.argv`` is parsed.
    :type args: argparse.Namespace or None
    :param kwargs: used when called programmatically; must contain at least
        ``src_file``.  All other keys (except ``src_file``) are forwarded to
        :meth:`pandas.DataFrame.to_csv`.
    '''

    if args is None:
        description = (
            "Convert a .feather or .parquet file to .csv.\n"
            "Options are the same as pandas.DataFrame.to_csv().\n"
            "The source format is detected automatically from the file extension."
        )

        parser = argparse.ArgumentParser(
            prog=os.path.basename(sys.argv[0]),
            formatter_class=argparse.RawTextHelpFormatter,
            description=description
        )
        parser.add_argument('src_file', help='Feather or parquet file to convert')
        parser.add_argument('--dst_file',
                            dest='path_or_buf',
                            default=None,
                            help='Output CSV path. Default: src path with .csv extension')
        parser.add_argument('--sep', default=',', help='field separator')
        parser.add_argument('--na_rep', default='', help='representation of NaN values')
        parser.add_argument('--float_format', default=None, help='format string for floating point numbers')
        parser.add_argument('--columns', default=None, help='columns to include')
        parser.add_argument('--header', type=str2bool, default=True, help='include the column names')
        parser.add_argument('--index', type=str2bool, default=True, help='write row names')
        parser.add_argument('--index_label', default=None, help='column header for the row names')
        parser.add_argument('--mode',
                            default='w',
                            help=("mode with which to write to the output. Default: 'w'\n"
                                  "   'w': truncate file first\n"
                                  "   'x': fail if file already exists\n"
                                  "   'a': append if file exists\n"))
        parser.add_argument('--encoding',
                            default=None,
                            help="Encoding to use in the output file; defaults to 'utf-8'.")
        parser.add_argument('--compression',
                            default='infer',
                            help="Detect compression from file extension.")
        parser.add_argument('--quoting', default=None)
        parser.add_argument('--quotechar', default='"')
        parser.add_argument('--lineterminator', default=None)
        parser.add_argument('--chunksize', default=None, help='rows to write at a time')
        parser.add_argument('--date_format', default=None, help='format string for datetime objects')
        parser.add_argument('--doublequote', type=str2bool, default=True)
        parser.add_argument('--escapechar', default=None)
        parser.add_argument('--decimal', default='.')
        parser.add_argument('--errors', default='strict')
        parser.add_argument('--storage_options', default=None)

        args = parser.parse_args()

        if not os.path.exists(args.src_file):
            print(f"File {args.src_file} not found")
            sys.exit(1)

        kwargs = args.__dict__

    src_file  = kwargs.pop('src_file')
    out_stream = kwargs.get('path_or_buf')

    if out_stream is None:
        dst_file   = Path(src_file).with_suffix('.csv')
        out_stream = open(dst_file, 'w')
        kwargs['path_or_buf'] = out_stream

    try:
        workhorse = FToolsWorkhorse(src_file, out_stream=out_stream)
        workhorse.df.to_csv(**kwargs)
    finally:
        if isinstance(out_stream, TextIOWrapper):
            out_stream.close()


if __name__ == '__main__':
    main()
