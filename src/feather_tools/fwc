#!/usr/bin/env python
'''
Created on May 6, 2024
@author: Andreas Paepcke

Emulates the Unix shell command 'wc -l' for .feather files.

Usage: fwc <feather-file-name>
'''

import argparse
import os
import sys

from feather_tools.ftools_workhorse import FToolsWorkhorse


def main(args=None, term_lines=None, term_cols=None, out_stream=sys.stdout):
    '''
    Create a feather-tools workhorse instance, and have it
    load the feather file into a dataframe.
    
    Arguments term_lines, term_cols, and out_stream are only
    needed during unittesting. 
    
    The args argument contains the command line argument: the path to 
    the feather file.
    
    The args fieldname is 'src_file' 
    
    :param args: argparse argument structure containing the path
        to the feather file. If None, will parse sys.argv.
    :type args: union[None | argparse.Namespace]
    :param term_lines: number of lines available on the current
        terminal. Used only during unittesting.
    :type term_lines: union[None | int]
    :param term_cols: number of characters available per line 
        on the current terminal. Used only during unittesting.
    :type term_cols: union[None | int]
    :param out_stream: stream to which output is to be written
        Used only during unittesting, but could be used for 
        writing to an arbitrary stream.
    :type out_stream: stream
    '''
    
    if args is None:
        # Parse command line arguments
        description = "Row counter for .feather files. Analogous to Unix tool wc -l"
        
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
    num_rows = len(workhorse.df)
    out_stream.write(f"{num_rows}\n")


if __name__ == '__main__':
    main()
    