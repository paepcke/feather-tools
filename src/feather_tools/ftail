#!/usr/bin/env python
'''
Created on May 6, 2024

@author: paepcke

Emulates the Unix shell command 'tail' for .feather files.

Usage: tail [{-l | --lines} <int>] <feather-file-name>

Note: displays the logical (i.e. terminal-height) page that
      contains the desired line. So a few more rows than
      requested may be displayed.

'''

import argparse
import os
import sys

from feather_tools.ftools_workhorse import FToolsWorkhorse

def main(args, term_lines=None, term_cols=None, out_stream=sys.stdout):
    '''
    Create a feather-tools workhorse instance, and have it
    load the feather file into a dataframe.
    
    Arguments term_lines, term_cols, and out_stream are only
    needed during unittesting. 
    
    The args argument must contain the command line arguments: 
    the path to the feather file, and the number of rows to 
    show.
    
    The args fields are 'src_file' and 'lines'.
    
    The args argument is created by argparse.
        
    :param args: argparse argument structure containing the path
        to the feather file, and number of rows
    :type args: namedtuple
    :param term_lines: number of lines available on the current
        terminal. Used only during unittesting.
    :type term_lines: union[None | int]
    :param term_cols: number of characters available per line 
        on the current terminal. Used only during unittesting.
    :type term_cols: union[None | int]
    :param out_stream: stream to which output is to be written
        Used only during unittesting, but could be used for 
        writing to an arbitrary stream.
    :type out_stream:
    '''
    
    workhorse = FToolsWorkhorse(args.src_file, lines=term_lines, cols=term_cols, out_stream=out_stream)
    pager = workhorse.pager
    num_rows = len(workhorse.df)
    lowest_row_to_show = num_rows - args.lines
    logical_page_low_row = pager.logical_page_by_row(lowest_row_to_show)
    
    page_num = logical_page_low_row
    while True:
        try:
            pager.show_page(page_num)
            page_num +=1 
        except ValueError:
            # Reached last logical page
            break

if __name__ == '__main__':
    description = "Show final rows .feather files. Analogous to Unix tool tail"

    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     description=description
                                     )

    parser.add_argument('-n', '--lines',
                        type=int,
                        help='number lines to show. Default: 10',
                        default=10)

    parser.add_argument('src_file',
                        help='File to of which to see tail')

    args = parser.parse_args()

    if not os.path.exists(args.src_file):
        print(f"File {args.src_file} not found")
        sys.exit()
        
    main(args)