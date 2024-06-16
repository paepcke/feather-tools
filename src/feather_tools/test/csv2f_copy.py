#!/usr/bin/env python
'''
Created on May 7, 2024

@author: paepcke

Writes the dataframe contained in a .csv file to
another file as .feather.

Usage: csv2f <csv_src_file> <feather-file-name> 
   may use switches appropriated for pd.read_csv()

'''

import argparse
import os
import sys
import pandas as pd
from pathlib import Path

from feather_tools.ftools_workhorse import FToolsWorkhorse
from numpy.lib._iotools import str2bool

#------------------------------------
# main 
#-------------------

def main(**kwargs):
    '''
    The kwargs argument must contain the command line argument 
    
         'src_file': path to the cvs file

    '''

    # Remove src_file and dst_file from the kwargs,
    # so that all the remaining kwargs can be passed
    # to pd.read_csv():
    
    src_file = kwargs.pop('src_file')
    try:
    	dst_file = kwargs.pop('dst_file')
    except KeyError:
        # No dst_file provided: use the src_file
        # with the .csv extension replaced with
        # .feather:
        src_path = Path(src_file)
        dst_file = src_path.with_suffix('.feather')
    
    # In pd.read_csv some args have default=_NoDefault.no_default
    # This means that if the command line arg is provided
    # then that _NoDefault.no_default must be replaced by some
    # value. Here we remove kwargs from the passed-in list if
    # argparse delivered the _NoDefault.no_default verbatim. It
    # means that the user did not provide that kwarg, so neither
    # will we:
    
    new_kwargs = {key : val
    			  for key, val
    			  in kwargs.items()
    			  if val != '_NoDefault.no_default'} 
    
    df = pd.read_csv(src_file, **new_kwargs)
    df.to_feather(dst_file)


if __name__ == '__main__':
    description = "Convert .feather file to .csv. Options are same as pandas.to_csv()"

    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     description=description
                                     )
    parser.add_argument('src_file',
                         help='File to convert')
    parser.add_argument('dst_file',
                         default='source file with extension changed to .feather',
                         help='File to which feather is written')
    # These remaining args are for pandas.read_csv()
    parser.add_argument('--delimiter',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--header',
                         default='infer',
                         help='see pandas.read_csv')
    parser.add_argument('--names',
                         default=_NoDefault.no_default,
                         help='see pandas.read_csv')
    parser.add_argument('--index_col',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--usecols',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--dtype',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--engine',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--converters',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--true_values',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--false_values',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--skipinitialspace',
                         default=False,
                         help='see pandas.read_csv')
    parser.add_argument('--skiprows',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--skipfooter',
                         default=0,
                         help='see pandas.read_csv')
    parser.add_argument('--nrows',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--na_values',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--keep_default_na',
                         default=True,
                         help='see pandas.read_csv')
    parser.add_argument('--na_filter',
                         default=True,
                         help='see pandas.read_csv')
    parser.add_argument('--verbose',
                         default=_NoDefault.no_default,
                         help='see pandas.read_csv')
    parser.add_argument('--skip_blank_lines',
                         default=True,
                         help='see pandas.read_csv')
    parser.add_argument('--parse_dates',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--infer_datetime_format',
                         default=_NoDefault.no_default,
                         help='see pandas.read_csv')
    parser.add_argument('--keep_date_col',
                         default=_NoDefault.no_default,
                         help='see pandas.read_csv')
    parser.add_argument('--date_parser',
                         default=_NoDefault.no_default,
                         help='see pandas.read_csv')
    parser.add_argument('--date_format',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--dayfirst',
                         default=False,
                         help='see pandas.read_csv')
    parser.add_argument('--cache_dates',
                         default=True,
                         help='see pandas.read_csv')
    parser.add_argument('--iterator',
                         default=False,
                         help='see pandas.read_csv')
    parser.add_argument('--chunksize',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--compression',
                         default='infer',
                         help='see pandas.read_csv')
    parser.add_argument('--thousands',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--decimal',
                         default='.',
                         help='see pandas.read_csv')
    parser.add_argument('--lineterminator',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--quotechar',
                         default='"',
                         help='see pandas.read_csv')
    parser.add_argument('--quoting',
                         default=0,
                         help='see pandas.read_csv')
    parser.add_argument('--doublequote',
                         default=True,
                         help='see pandas.read_csv')
    parser.add_argument('--escapechar',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--comment',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--encoding',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--encoding_errors',
                         default='strict',
                         help='see pandas.read_csv')
    parser.add_argument('--dialect',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--on_bad_lines',
                         default='error',
                         help='see pandas.read_csv')
    parser.add_argument('--delim_whitespace',
                         default=_NoDefault.no_default,
                         help='see pandas.read_csv')
    parser.add_argument('--low_memory',
                         default=True,
                         help='see pandas.read_csv')
    parser.add_argument('--memory_map',
                         default=False,
                         help='see pandas.read_csv')
    parser.add_argument('--float_precision',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--storage_options',
                         default=None,
                         help='see pandas.read_csv')
    parser.add_argument('--dtype_backend',
                         default=_NoDefault.no_default,
                         help='see pandas.read_csv')

    args = parser.parse_args()

    if not os.path.exists(args.src_file):
        print(f"File {args.src_file} not found")
        sys.exit()
        
    args_dict = args.__dict__
    
    main(**args_dict)
    
