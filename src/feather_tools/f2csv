#!/usr/bin/env python
'''
Created on May 7, 2024
@author: Andreas Paepcke

Writes the dataframe contained in a .feather file to
standard out as a .csv file. 

Usage: f2csv [{-s | --separator} <str>] <feather-file-name> 
'''

import argparse
import os
import sys
from pathlib import Path
from io import TextIOWrapper

from feather_tools.ftools_workhorse import FToolsWorkhorse


def str2bool(bool_str):
    '''
    Convert string representation of boolean to actual boolean.
    
    :param bool_str: string or boolean value
    :return: boolean value
    :raises argparse.ArgumentTypeError: if string cannot be converted
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
    Create a feather-tools workhorse instance, and have it
    load the feather file into a dataframe, then write as CSV.
    
    Argument out_stream is only needed during unittesting. 
    
    The kwargs argument must contain the command line arguments: 
    the path to the feather file, and the separator.
    
    The args fields are 'src_file' and 'path_or_buf'.
    
    :param args: argparse argument structure. If None, will parse sys.argv.
    :type args: union[None | argparse.Namespace]
    :param kwargs: additional keyword arguments for pd.to_csv() when called
                   programmatically (for testing)
    '''
    
    if args is None:
        # Parse command line arguments
        description = "Convert .feather file to .csv. Options are same as pandas.to_csv()"

        parser = argparse.ArgumentParser(
            prog=os.path.basename(sys.argv[0]),
            formatter_class=argparse.RawTextHelpFormatter,
            description=description
        )
        parser.add_argument('src_file', help='File to convert')
        parser.add_argument('--dst_file',
                           dest='path_or_buf',
                           default=None,
                           help='File to which csv is written. Default: source file with .csv extension')
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
                           help="A string representing the encoding to use in the output file, defaults to 'utf-8'.")
        parser.add_argument('--compression',
                           default='infer',
                           help=("detect compression from file extension: '.gz', '.bz2', '.zip',\n"
                                 "'.xz', '.zst', '.tar', '.tar.gz', '.tar.xz' or '.tar.bz2'. "))
        parser.add_argument('--quoting', default=None)
        parser.add_argument('--quotechar', default='"', help='string of length 1: char used in quote fields. Default: \'"\'')
        parser.add_argument('--lineterminator', default=None, help='newline char or char sequence. Defaults to os.linesep')
        parser.add_argument('--chunksize', default=None, help='rows to write at a time')
        parser.add_argument('--date_format', default=None, help='format string for datetime objects')
        parser.add_argument('--doublequote', type=str2bool, default=True, help='control quoting of quotechar inside fields')
        parser.add_argument('--escapechar', default=None, help='string of length 1. Used to escape sep and quotechar')
        parser.add_argument('--decimal', default='.', help='char recognized as decimal point')
        parser.add_argument('--errors', default='strict', help="specifies encoding and decoding errors. Default: 'strict'")
        parser.add_argument('--storage_options', default=None, help='extra options for storage connections')

        args = parser.parse_args()

        if not os.path.exists(args.src_file):
            print(f"File {args.src_file} not found")
            sys.exit(1)
        
        kwargs = args.__dict__

    src_file = kwargs.pop('src_file')

    out_stream = kwargs['path_or_buf']
    if out_stream is None:
        dst_file = Path(src_file).with_suffix('.csv')
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