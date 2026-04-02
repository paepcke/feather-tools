#!/usr/bin/env python
'''
Created on May 6, 2024
@author: Andreas Paepcke

Emulates the Unix shell command 'tail' for .feather and .parquet files.
The file format is detected automatically from the file extension.

Both the ``ftail`` and ``ptail`` entry points call the same function.

Usage: ftail [{-n | --lines} <int>] <feather-or-parquet-file>
       ptail [{-n | --lines} <int>] <feather-or-parquet-file>

Note: displays the logical (i.e. terminal-height) page that contains
      the desired row, so a few more rows than requested may appear.
'''

import argparse
import os
import sys

from feather_tools.ftools_workhorse import FToolsWorkhorse


def main(args=None, term_lines=None, term_cols=None, out_stream=sys.stdout):
    '''
    Show the final rows of a feather or parquet file.

    The source format is inferred from the file-name extension;
    both ``ftail`` and ``ptail`` call this function.

    :param args: pre-parsed argparse namespace with attributes ``src_file``
        (path to data file) and ``lines`` (number of rows to show).
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
            "Show final rows of .feather or .parquet files.\n"
            "Analogous to the Unix tool 'tail'."
        )

        parser = argparse.ArgumentParser(
            prog=os.path.basename(sys.argv[0]),
            formatter_class=argparse.RawTextHelpFormatter,
            description=description
        )
        parser.add_argument('-n', '--lines',
                            type=int,
                            help='number of rows to show (default: 10)',
                            default=10)
        parser.add_argument('src_file',
                            help='Feather or parquet file to tail')

        args = parser.parse_args()

        if not os.path.exists(args.src_file):
            print(f"File {args.src_file} not found")
            sys.exit(1)

    workhorse = FToolsWorkhorse(args.src_file, lines=term_lines, cols=term_cols, out_stream=out_stream)
    pager     = workhorse.pager
    num_rows  = len(workhorse.df)

    lowest_row_to_show  = num_rows - args.lines
    logical_page_low_row = pager.logical_page_by_row(lowest_row_to_show)

    page_num = logical_page_low_row
    while True:
        try:
            pager.show_page(page_num)
            page_num += 1
        except ValueError:
            break


if __name__ == '__main__':
    main()
