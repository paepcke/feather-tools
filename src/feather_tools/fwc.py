#!/usr/bin/env python
'''
Created on May 6, 2024
@author: Andreas Paepcke

Emulates the Unix shell command 'wc -l' for .feather and .parquet files.
The file format is detected automatically from the file extension.

Both the ``fwc`` and ``pwc`` entry points call the same function.

Usage: fwc <feather-or-parquet-file>
       pwc <feather-or-parquet-file>
'''

import argparse
import os
import sys

from feather_tools.ftools_workhorse import FToolsWorkhorse


def main(args=None, term_lines=None, term_cols=None, out_stream=sys.stdout):
    '''
    Print the number of rows in a feather or parquet file.

    The source format is inferred from the file-name extension;
    both ``fwc`` and ``pwc`` call this function.

    :param args: pre-parsed argparse namespace with a ``src_file`` attribute.
        If None, ``sys.argv`` is parsed.
    :type args: argparse.Namespace or None
    :param term_lines: override terminal height (used in unit tests)
    :type term_lines: int or None
    :param term_cols: override terminal width (used in unit tests)
    :type term_cols: int or None
    :param out_stream: output stream (used in unit tests)
    :type out_stream: file-like
    '''

    if args is None:
        description = (
            "Row counter for .feather and .parquet files.\n"
            "Analogous to the Unix tool 'wc -l'."
        )

        parser = argparse.ArgumentParser(
            prog=os.path.basename(sys.argv[0]),
            formatter_class=argparse.RawTextHelpFormatter,
            description=description
        )
        parser.add_argument('src_file', help='File to count rows in')

        args = parser.parse_args()

        if not os.path.exists(args.src_file):
            print(f"File {args.src_file} not found")
            sys.exit(1)

    workhorse = FToolsWorkhorse(args.src_file, lines=term_lines, cols=term_cols, out_stream=out_stream)
    num_rows  = len(workhorse.df)
    out_stream.write(f"{num_rows}\n")


if __name__ == '__main__':
    main()
