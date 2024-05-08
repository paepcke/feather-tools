#!/usr/bin/env python
'''
Created on May 6, 2024

@author: paepcke

Emulates the Unix shell command 'wc -l' for .feather files.

Usage: fwc <feather-file-name>

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
    
    The args argument contains the command line argument: the path to 
    the feather file.
    
    The args fieldname is 'src_file' 
    
    The args argument is created by argparse.
        
    :param args: argparse argument structure containing the path
        to the feather file.
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
    num_rows = len(workhorse.df)
    out_stream.write(f"{num_rows}\n")

# ------------------------ Main ------------
if __name__ == '__main__':
    
    description = "Row counter for .feather files. Analogous to Unix tool wc -l"

    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     description=description
                                     )

    parser.add_argument('src_file',
                        help='File to view')

    args = parser.parse_args()

    if not os.path.exists(args.src_file):
        print(f"File {args.src_file} not found")
        sys.exit()
            
    main(args   )