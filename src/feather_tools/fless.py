#!/usr/bin/env python
'''
Created on May 6, 2024
@author: Andreas Paepcke

Emulates the Unix shell command 'less' for .feather and .parquet files.
The file format is detected automatically from the file extension.

Both the ``fless`` and ``pless`` entry points call the same function.

Usage: fless <feather-or-parquet-file>
       pless <feather-or-parquet-file>

After each page:
    - To show the next page : spacebar, ENTER, or 'n'
    - Back one page         : b
    - Back to beginning     : s
    - To the last page      : e
    - For help              : h
    - To quit               : q
'''
import argparse
import os
import sys

from feather_tools.ftools_workhorse import FToolsWorkhorse


def main(args=None, term_lines=None, term_cols=None, out_stream=sys.stdout):
    '''
    Page through a feather or parquet file in the terminal.

    The source format is inferred from the file-name extension;
    both ``fless`` and ``pless`` call this function.

    :param args: pre-parsed argparse namespace whose ``src_file`` attribute
        is the path to the data file.  If None, ``sys.argv`` is parsed.
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
            "Provides a Unix 'less' facility for .feather and .parquet files.\n"
            "After each display page, use:\n"
            "  - Next page        : n, spacebar, or ENTER\n"
            "  - Previous page    : b\n"
            "  - Beginning of file: s\n"
            "  - End of file      : e\n"
            "  - Help             : h\n"
            "  - Quit displaying  : q"
        )

        parser = argparse.ArgumentParser(
            prog=os.path.basename(sys.argv[0]),
            formatter_class=argparse.RawTextHelpFormatter,
            description=description
        )
        parser.add_argument('src_file', help='Feather or parquet file to view')
        args = parser.parse_args()

    if not os.path.exists(args.src_file):
        print(f"File {args.src_file} not found")
        sys.exit(1)

    workhorse = FToolsWorkhorse(
        args.src_file,
        lines=term_lines,
        cols=term_cols,
        out_stream=out_stream
    )
    workhorse.page()


if __name__ == '__main__':
    main()
