#!/usr/bin/env python
'''
Created on May 6, 2024

@author: paepcke

Emulates the Unix shell command 'less' for .feather files.
The implementation loads the entire .feather file to do its
work.

Usage: more <feather-file-name>

After each page:
	- To show the next page: spacebar or '\n', or the character 'n'
	- Back one page: b
	- Back to beginning (page 0): s
	- To the last page: e
	- For help: h
	- To quit the display: q
	

'''
import argparse
import os
import sys

from feather_tools.ftools_workhorse import FToolsWorkhorse

def main(args, term_lines=None, term_cols=None, out_stream=sys.stdout):
    '''
    Create a feather-tools workhorse instance, and have it
    page through the feather file.
    
    Arguments term_lines, term_cols, and out_stream are only
    needed during unittesting. The args argument contains the 
    command line argument: the path to the feather file. 
    
    The args field 'src_file'.

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
    workhorse.page()

if __name__ == '__main__':
    description = ("Provides a Unix 'more' a.k.a. 'less' facility for .feather files.\n"
                   "After each display page, use:\n"
                   "   - Next page    : n, spacebar, or ENTER\n"
                   "   - Previous page: b\n"
                   "   - Beginning of file: s\n"
                   "   - End of file      : e\n"
                   "   - Help             : h\n"
                   "   - Quit displaying  : q")
    
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
    
    main(args)