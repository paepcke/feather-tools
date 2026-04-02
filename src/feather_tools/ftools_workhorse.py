'''
Created on May 1, 2024
@author: Andreas Paepcke

TODO:
   o Long rows: early rows not shown
   o 'begin' ('B', 'b' commands) don't show the columns again.
   o getchr() works in interactive Python shell, but not
       in Eclipse or inside FToolsWorkhorse application
'''
from pathlib import Path
import bisect
import io
import os
import pandas as pd
import random
import shutil
import sys
import termios
import tty

# Supported tabular formats and their file extensions:
FEATHER_EXTS  = {'.feather'}
PARQUET_EXTS  = {'.parquet', '.pq'}
SUPPORTED_EXTS = FEATHER_EXTS | PARQUET_EXTS


def infer_format(path):
    '''
    Return the storage format implied by *path*'s file extension.

    :param path: path to a tabular data file
    :type path: str or Path
    :return: ``'feather'`` or ``'parquet'``
    :rtype: str
    :raises ValueError: if the extension is not recognised
    '''
    ext = Path(path).suffix.lower()
    if ext in FEATHER_EXTS:
        return 'feather'
    if ext in PARQUET_EXTS:
        return 'parquet'
    raise ValueError(
        f"Unrecognised file extension '{ext}'. "
        f"Supported extensions: {sorted(SUPPORTED_EXTS)}"
    )


def default_format_from_invocation():
    '''
    Inspect ``sys.argv[0]`` to determine which format family the user
    invoked (``f``-series → feather, ``p``-series → parquet).

    The heuristic is: if the command name starts with ``'p'`` (e.g.
    ``pless``, ``ptail``, ``pwc``, ``p2csv``, ``csv2p``) the caller
    wants parquet; otherwise feather is assumed.

    :return: ``'feather'`` or ``'parquet'``
    :rtype: str
    '''
    cmd = Path(sys.argv[0]).name.lower()
    if cmd.startswith('p') or cmd.endswith('2p'):
        return 'parquet'
    return 'feather'


def load_df(path):
    '''
    Load a feather or parquet file into a :class:`pandas.DataFrame`,
    dispatching on the file extension.

    :param path: path to the data file
    :type path: str or Path
    :return: the loaded dataframe
    :rtype: pd.DataFrame
    :raises ValueError: if the extension is not recognised
    '''
    fmt = infer_format(path)
    if fmt == 'feather':
        return pd.read_feather(path)
    return pd.read_parquet(path)


#  -------------------------- Class FToolsWorkhorse --------------

class FToolsWorkhorse:
    '''
    Loads a feather or parquet file into a DataFrame and provides
    paged terminal display.  The file format is inferred automatically
    from the file-name extension.
    '''

    def __init__(self, path, lines=None, cols=None, out_stream=sys.stdout, unittesting=False):
        '''
        :param path: location of the tabular data file (.feather, .parquet, or .pq).
            May be absolute or relative to the current directory.
        :type path: str
        :param lines: number of lines per page. If None, use terminal height.
        :type lines: int or None
        :param cols: number of character columns per line.
            If None, use terminal width.
        :type cols: int or None
        :param out_stream: where to direct output. Default is stdout.
        :type out_stream: file-like
        :param unittesting: if True, only minimal data initialisation is
            performed; no pager is constructed.
        :type unittesting: bool
        '''

        self.help_str = 'cr or spacebar: next; b: back; s: start; e: end; q: quit. (Any key to continue...)'

        self.term_cols, self.term_lines = shutil.get_terminal_size()
        if lines is not None:
            self.term_lines = lines
        if cols is not None:
            self.term_cols = cols

        self.out_stream = out_stream

        if type(path) != str:
            raise TypeError(f"Path must be string or file-like, not {type(path)}")

        cwd = Path(os.getcwd())
        if os.path.isabs(path):
            self.path = Path(path)
        else:
            self.path = cwd.joinpath(path)

        # Prompt after each page: if path is cwd, just
        # use the file name else the whole path:
        if self.path.parent == cwd.parent:
            self.prompt = self.path.name
        else:
            self.prompt = str(self.path)

        try:
            self.df = load_df(self.path)
        except Exception as _e:
            print(f"Cannot find or open file: {_e}")
            sys.exit()

        if unittesting:
            return

        self.pager = Pager(self.df, self.term_lines, term_cols=self.term_cols, out_stream=self.out_stream)

    #------------------------------------
    # page
    #-------------------

    def page(self):
        '''
        Pages through self.df until user enters 'q'.
        '''
        while True:
            try:
                next(self.pager)
            except StopIteration:
                action_char = self.pager.getchr(self.prompt + '(END)')
            else:
                action_char = self.pager.getchr(self.prompt)

            if action_char in ['\n', u"\u0020", 'n']:
                continue
            if action_char in ['Q', 'q']:
                return
            if action_char in ['B', 'b']:
                self.pager.back_one_page()
                continue
            if action_char in ['S', 's']:
                self.pager.beginning()
                continue
            if action_char in ['E', 'e']:
                self.pager.end()
                continue
            if action_char in ['H', 'h']:
                self.out_stream.write(self.help_str + '\n')
                self.pager.getchr()
                continue


#  -------------------------- Class Pager --------------

class Pager:
    '''
    Creates dict of logical display page to range of rows
    in a dataframe. Provides method to extract rows by
    logical page.
    '''

    #------------------------------------
    # Constructor
    #-------------------

    def __init__(self, df, term_lines, term_cols=80, out_stream=sys.stdout, unittesting=False):
        '''
        :param df: dataframe to page through
        :type df: pd.DataFrame
        :param term_lines: number of lines that can be displayed on terminal
        :type term_lines: int
        :param term_cols: number of columns in the terminal
        :type term_cols: int
        :param out_stream: where to write output
        :type out_stream: file-like
        :param unittesting: if True, only initialises some constants;
            no computations performed.
        :type unittesting: bool
        '''

        self.df = df
        self.term_cols = term_cols
        self.term_lines = term_lines
        self.out_stream = out_stream
        self.cur_page = 0

        self.inter_column_padding = 4
        self.at_end = False

        if unittesting:
            return

        num_col_lines, self.data_lines_per_page = self._compute_lines_per_page(df)

        self.pindex = self._pagination_index(
            self.df,
            self.data_lines_per_page,
            num_col_lines)

    #------------------------------------
    # logical_page_by_row
    #-------------------

    def logical_page_by_row(self, row_num):
        '''
        Given a row number in our dataframe, return the
        number of the logical page where the row is included.

        If the row number is larger than the length of the
        dataframe, return the last logical page number.

        :param row_num: number of row in the dataframe
        :type row_num: int
        :return: number of logical page where the row occurs
        :rtype: int
        '''
        if row_num >= len(self.df):
            return self.pages_list[-1]

        page_num = bisect.bisect_left(self.pages_list, row_num, key=lambda key: self.pindex[key][1] - 1)
        return page_num

    #------------------------------------
    # _pagination_index
    #-------------------

    def _pagination_index(self, df, data_lines_per_page, num_col_lines):
        '''
        Constructs an index from logical display page to a dataframe row range.

        :param df: dataframe to page
        :type df: pd.DataFrame
        :param data_lines_per_page: number of pure data lines that fit on the
            current terminal (not counting the column header)
        :type data_lines_per_page: int
        :param num_col_lines: number of lines required for the column header on page 0
        :type num_col_lines: int
        :return: mapping pagenum → (df-start-row, df-stop-row)
        :rtype: dict
        '''
        pcache = {}

        data_space = self.term_lines - num_col_lines
        term_lines_per_data = max(int(self.term_lines / data_lines_per_page), 1)

        data_lines_p0 = min(len(df), int(data_space / term_lines_per_data))
        pcache[0] = (0, data_lines_p0)
        start_row_p1 = data_lines_p0

        for page_num, row_num in enumerate(range(start_row_p1, len(df), data_lines_per_page)):
            page_num += 1
            upper_row = min(row_num + data_lines_per_page, len(df))
            pcache[page_num] = (row_num, upper_row)

        self.pages_list = list(pcache.keys())
        return pcache

    #------------------------------------
    # _compute_lines_per_page
    #-------------------

    def _compute_lines_per_page(self, df):
        '''
        Estimates the number of dataframe rows that fit on one terminal page,
        accounting for column-header height and possible line wrapping.

        :param df: dataframe to examine
        :type df: pd.DataFrame
        :return: (num_col_header_lines, data_rows_per_page)
        :rtype: tuple[int, int]
        '''
        cols_str = (' ' * self.inter_column_padding).join(df.columns)
        num_cols_lines = self._num_wrapped_lines(None, cols_str)

        num_cols = len(self.df.columns)
        one_col_width = self._estimate_col_print_width(self.df, self.inter_column_padding)

        fake_col = 'a' * (one_col_width - self.inter_column_padding) + ' ' * self.inter_column_padding
        fake_row_str = (fake_col * num_cols).strip()
        num_data_lines = self._num_wrapped_lines(len(self.df), fake_row_str)

        lines_per_page = max(int(self.term_lines / num_data_lines), 1)

        return num_cols_lines, lines_per_page

    #------------------------------------
    # _estimate_col_print_width
    #-------------------

    def _estimate_col_print_width(self, df, padding=None):
        '''
        Returns an estimate of the maximum printed width of any single column
        value, sampled from a few rows to keep cost low.

        :param df: dataframe to examine
        :type df: pd.DataFrame
        :param padding: number of inter-column space characters to add to the
            returned width
        :type padding: int or None
        :return: estimated maximum column print width including padding
        :rtype: int
        '''
        if padding is None:
            padding = self.inter_column_padding
        num_samples = 4
        if len(df) <= num_samples:
            row_nums = range(0, len(df))
        else:
            row_nums = random.sample(range(0, len(df)), k=num_samples)

        global_print_width = 0
        for i in row_nums:
            row = df.iloc[i]
            print_widths = [len(str(value)) for value in row.values]
            global_print_width = max(global_print_width, max(print_widths))

        return global_print_width + padding

    #------------------------------------
    # __iter__
    #-------------------

    def __iter__(self):
        return self

    #------------------------------------
    # __next__
    #-------------------

    def __next__(self):
        if self.cur_page >= len(self.pindex):
            self.at_end = True
            self.cur_page = len(self.pindex)
            raise StopIteration()

        self.show_page(self.cur_page)
        self.cur_page += 1

    #------------------------------------
    # show_page
    #-------------------

    def show_page(self, page_num):
        '''
        Display the dataframe rows that belong to the given logical page.
        On page 0 the column header is printed first.

        :param page_num: logical page number to display
        :type page_num: int
        :raises ValueError: if page_num is out of range
        '''
        if type(page_num) != int or page_num > self.pages_list[-1]:
            raise ValueError(f"Logical page number must be an int between 0 and {self.pages_list[-1]}")

        pad_spaces = ' ' * self.inter_column_padding

        if page_num == 0:
            col_str = pad_spaces.join(self.df.columns)
            self._write_tab_row(None, col_str)

        start_row, stop_row = self.pindex[page_num]
        df_excerpt = self.df.iloc[start_row:stop_row]

        for row_num, row in df_excerpt.iterrows():
            val_strings = [str(val) for val in row.values]
            row_str = pad_spaces.join(val_strings)
            self._write_tab_row(row_num, row_str)

    #------------------------------------
    # _num_wrapped_lines
    #-------------------

    def _num_wrapped_lines(self, row_num, line):
        '''
        Return the number of terminal lines that *line* will occupy after
        word-wrapping at the current terminal width.

        :param row_num: row index used as the leading label, or None for
            the column header
        :type row_num: int or None
        :param line: the string to measure
        :type line: str
        :return: number of terminal lines the string occupies
        :rtype: int
        '''
        buf = io.StringIO()
        saved_stream = self.out_stream
        try:
            self.out_stream = buf
            sys.stderr = buf
            self._write_tab_row(row_num, line)
            wrapped_str = buf.getvalue()
        finally:
            buf.close()
            self.out_stream = saved_stream
            sys.stderr = sys.__stderr__

        num_term_lines = wrapped_str.count('\n')
        return num_term_lines

    #------------------------------------
    # _write_tab_row
    #-------------------

    def _write_tab_row(self, row_num, row_str):
        '''
        Write one row to ``self.out_stream``, wrapping at word boundaries
        if the content is wider than the terminal.

        :param row_num: the row index, or None when writing the column header
        :type row_num: int or str or None
        :param row_str: the row content to write
        :type row_str: str
        '''
        if type(row_num) == int:
            row_num = str(row_num)

        if row_num is None:
            row_num_str = ''
        else:
            row_num_str = f"{row_num}: "
        row_num_width = len(row_num_str)
        if row_num_width + len(row_str) <= self.term_cols:
            self.out_stream.write(f"{row_num_str}{row_str}\n")
            return

        indent = ' ' * row_num_width
        is_first_line = True
        while True:
            max_break_pt = self.term_cols - row_num_width
            if len(row_str) <= max_break_pt or row_str[max_break_pt] == ' ':
                good_break_pt = max_break_pt
            else:
                good_break_pt = row_str[:max_break_pt].rfind(' ')
            if good_break_pt == -1 and len(row_str) >= max_break_pt:
                break
            if is_first_line:
                self.out_stream.write(f"{row_num_str}{row_str[:good_break_pt]}\n")
                is_first_line = False
            else:
                self.out_stream.write(f"{indent}{row_str[:good_break_pt]}\n")
            row_str = row_str[good_break_pt:].strip()
            if len(row_str) == 0:
                return

        if is_first_line:
            self.out_stream.write(f"{row_num_str}{row_str}\n")
        else:
            self.out_stream.write(f"{indent}{row_str}\n")

    #------------------------------------
    # back_one_page
    #-------------------

    def back_one_page(self):
        '''Move the cursor back one page.'''
        self.cur_page = max(0, self.cur_page - 2)
        self.out_stream.write('\n')

    #------------------------------------
    # beginning
    #-------------------

    def beginning(self):
        '''Reset cursor to the first page.'''
        self.cur_page = 0
        self.out_stream.write('\n')

    #------------------------------------
    # end
    #-------------------

    def end(self):
        '''Jump to the last page.'''
        self.cur_page = len(self.pindex) - 1
        self.out_stream.write('\n')

    #------------------------------------
    # getchr
    #-------------------

    def getchr(self, prompt=''):
        '''
        Read a single keystroke without requiring Enter, falling back to
        line-buffered input when not running on a real TTY (e.g. in an IDE).

        :param prompt: optional prompt written on the same line as the cursor
        :type prompt: str
        :return: the character typed by the user
        :rtype: str
        '''
        if not sys.stdin.isatty():
            res_str = ''
            while len(res_str) != 1:
                res_str = input(prompt)
            return res_str

        sys.stdout.write(prompt)
        sys.stdout.flush()
        fd = sys.stdin.fileno()
        saved_tty = termios.tcgetattr(fd)
        try:
            tty.setcbreak(fd, termios.TCSANOW)
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSANOW, saved_tty)

        print('\r', end="")
        return ch
