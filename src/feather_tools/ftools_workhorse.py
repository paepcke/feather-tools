'''
Created on May 1, 2024

@author: paepcke

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

#  -------------------------- Class FToolsWorkhorse --------------
            
class FToolsWorkhorse:
    '''
    classdocs
    '''

    def __init__(self, path, lines=None, cols=None, out_stream=sys.stdout, unittesting=False):
        '''
        
        
        :param path: location of dataframe .feather_tools file. Maybe
            absolute, or relative to current directory
        :type path: str
        :param lines: number of lines per page. If None, 
            use terminal height
        :type lines: union[None | int]
        :param cols: number character columns for each line.
            If None, use terminal width
        :type cols: union[None | int}
        :param out_stream: where to direct the output. Default
            is stdout
        :type out_stream: file-like 
        :param unittesting: whether or not instantiation is for
            unittesting individual methods. If True, only minimal
            data init is done in the __init__() method.
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
            self.df = pd.read_feather(self.path)
        except Exception as _e:
            print("Cannot find or open file")
            sys.exit()
        
        if unittesting:
            return 
        
        self.pager = Pager(self.df, self.term_lines, term_cols=self.term_cols, out_stream=self.out_stream)
        
        #self.page()
        
    #------------------------------------
    # page
    #-------------------
    
    def page(self):
        '''
        Pages through self.df until user enters 'q'
        '''
        while True:
            # Print one page
            try:
                next(self.pager)
            except StopIteration:
                action_char = self.pager.getchr(self.prompt + '(END)')
            else:
                action_char = self.pager.getchr(self.prompt)
            
            # Return or spacebar?
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
                # Help info:
                self.out_stream.write(self.help_str + '\n')
                # Wait for user to be done reading:
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
        The safety margin is the number of terminal lines to
        not use. For example, to ensure room for the columns
        on page zero.
        
        :param df: dataframe to page through
        :type df: pd.DataFrame
        :param term_lines: number of lines that can be displayed
            on terminal
        :type term_lines: int
        :param term_cols: number of columns in the terminal
            on which pages will be shown
        :type term_cols: int
        :param out_stream: where to write output
        :type out_stream: file-like
        :param unittesting: if True, only initializes some constants;
            no computations performed.
        :type unittesting: bool
        '''
        
        self.df = df
        self.term_cols = term_cols
        self.term_lines = term_lines
        self.out_stream = out_stream
        self.cur_page = 0
        
        # Number of spaces between columns
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
        
        If the row number is larger than length of the 
        dataframe, return the last logical page number.
        
        :param row_num: number of row in the dataframe
        :type row_num: int
        :return number of logical page where the row occurs
        :rtype int
        '''

        # If row number beyond last row of the df,
        # return the last logical page number: 
        if row_num >= len(self.df):
            return self.pages_list[-1]
        
        page_num = bisect.bisect_left(self.pages_list, row_num, key=lambda key : self.pindex[key][1]-1)

        return page_num

    #------------------------------------
    # pagination_cache
    #-------------------
        
    def _pagination_index(self, df, data_lines_per_page, num_col_lines):
        '''
        Constructs an index from logical display page
        to a dataframe row range.
        
        For page 0, figures in room for the column header.
        
        Must first call _compute_lines_per_page() to initialize
        values needed here.
        
        :param df: dateframe to page 
        :type df: pd.DataFrame
        :param data_lines_per_page: number of pure data lines
            that fit on the current terminal. I.e. not counting
            the column header.
        :type data_lines_per_page: int
        :param num_col_lines: number of lines required for
            the column header on page 0
        :type num_col_lines: int
        :return an index pagenum ==> (df-start-row, df-stop-row)
        '''
        
        pcache = {}
        
        # Compute number of lines for page 0: num_col_lines
        # are taken by the column:
        data_space = self.term_lines - num_col_lines
        
        # Since each page accommodates data_lines_per_page,
        # one dataline occupies:
        term_lines_per_data = max(int(self.term_lines / data_lines_per_page), 1)
                
        data_lines_p0 = min(len(df), int(data_space / term_lines_per_data))
        pcache[0] = (0, data_lines_p0)
        start_row_p1 = data_lines_p0 
        
        # Go through the df in strides of the number of df rows
        # that fit on the followup pages:
        for page_num, row_num in enumerate(range(start_row_p1, len(df), data_lines_per_page)):
            # Took care of page 0 before the loop:
            page_num += 1
            
            # The upper row gets a -1 to accommodate the
            # column header:
            upper_row = min(row_num + data_lines_per_page, len(df))
            # Lines per page could be more than 
            # the length of the df. Therefore the min():
            pcache[page_num] = (row_num, upper_row)

        # Convenience list of the logical page range:
        self.pages_list = list(pcache.keys())
        return pcache

    #------------------------------------
    # _compute_lines_per_page
    #-------------------
    
    def _compute_lines_per_page(self, df):
        '''
        Estimates the number of lines to show on 
        one page, given term window width and height
        in self.term_cols and self.term_lines.
        
        If neither data nor the column header exceed the
        terminal width, then the number of lines to show
        equals the terminal height. I.e. no wrapping.
        
        Returns two numbers: the number of (possibly wrapped)
        terminal lines that the column header occupie.
        
        The second result is the number of dataframe rows
        to show on one page. The number is estimated by 
        computing the number of wrap lines needed for
        a sample data row from the df.    
        
        :param df: dataframe to examine
        :type df: pd.DataFrame
        :returned number of terminal lines needed for the column
            header, and the total number of rows that can be
            displayed on one terminal screen.
        :rtype tuple[int, int]
        
        '''
        # Compute a safety margins of lines to use per
        # page, given the possibility of line overruns
        # into the next line:
        
        # Start with the column header:
        cols_str = (' '*self.inter_column_padding).join(df.columns)
        # How many lines will the column header take?
        num_cols_lines = self._num_wrapped_lines(None, cols_str)
        
        num_cols = len(self.df.columns)
        one_col_width = self._estimate_col_print_width(self.df, self.inter_column_padding)
        
        fake_col = 'a'*(one_col_width - self.inter_column_padding) + ' '*self.inter_column_padding
        fake_row_str = (fake_col * num_cols).strip()
        # Get number of terminal lines taken by one column, 
        # using the largest row number as the row_number for 
        # the line wrapper:
        num_data_lines = self._num_wrapped_lines(len(self.df), fake_row_str)
        
        # Number of data lines to fit on the current
        # terminal:
        lines_per_page = max(int(self.term_lines / num_data_lines),1)
        
        return num_cols_lines, lines_per_page
           
    #------------------------------------
    # _estimate_col_print_width
    #-------------------
    
    def _estimate_col_print_width(self, df, padding=None):
        '''
        Returns the number of chars likely to be taken
        up one by column when it is printed. This width is
        an estimate, since it is undesirable to test columns
        of all rows in the df. 
        
        Instead, several samples are taken, the max column
        print width of each row is determined, and the overall
        max is returned.
        
        The returned number includes pading number of characters
        to account for inter-column empty space
         
        :param df: dataframe to examine
        :type df: pd.DataFrame
        :param padding: optional number of space characters
            to add to the returned col width
        :type padding: int
        :returned the df's estimated maximum column print width
        :rtype int
        '''
        if padding is None:
            padding = self.inter_column_padding
        num_samples = 4
        row_nums = set()
        if len(df) <= num_samples:
            # Just test the whole df:
            row_nums = range(0, len(df))
        else:
            row_nums = random.sample(range(0, len(df)), k=num_samples)
        
        global_print_width = 0
        for i in row_nums:
            row = df.iloc[i]
            print_widths = [len(str(value))
                            for value 
                            in row.values]
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
        
        # Have we displayed the last page?
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
        Given the number of a logical page, display df
        rows for that page. If page_num is zero, then
        first print the column headers.
        
        :param page_num: logical page number to display
        :type page_num: int
        '''
        
        if type(page_num) != int or page_num > self.pages_list[-1]:
            raise ValueError(f"Logical page number must be an in between 0 and {self.pages_list[-1]}")
        
        # String for empty space between column names,
        # or column values:
        pad_spaces = ' '*self.inter_column_padding

        if page_num == 0:
            # Print the col header first:
            col_str = pad_spaces.join(self.df.columns)
            # No leading row number, therefore the None:
            self._write_tab_row(None, col_str)
        
        start_row, stop_row = self.pindex[page_num]
        df_excerpt = self.df.iloc[start_row : stop_row]
        
        for row_num, row in df_excerpt.iterrows():
            # Get array of strings from the row values:
            val_strings = [str(val) for val in row.values]
            # Separate the col values by pad_spaces
            row_str = pad_spaces.join(val_strings)
            self._write_tab_row(row_num, row_str)
        

    
    #------------------------------------
    # _num_wrapped_lines
    #-------------------
    
    def _num_wrapped_lines(self, row_num, line):
        '''
        Given a, possibly long string, return the number
        of lines the string will occupy on the current
        terminal. 
        
        Strategy: Uses _write_tab_row, but writing to
        a string buffer, where \n occurrences can be counted.
         
        :param line: line to examine
        :type line: str
        :return number of terminal lines taken by the given string,
        :rtype int
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
        
        # Count number of newlines (of which there
        # will be one at the end)
        num_term_lines = wrapped_str.count('\n')
        return num_term_lines

    #------------------------------------
    # _write_tab_row
    #-------------------
    
    def _write_tab_row(self, row_num, row_str):
        '''
        Given a row number, and a possibly long
        string, break the string into multiple lines,
        and line the lines up for left-justification.
        Wrapping occurs only at spaces in the string.
        See 'Shortcoming' below for a corner case
        consequence.
        
        Print the row number with a colon, then the 
        broken-up row values.
        
        If row_num is None, such as for the column
        header, then no leading row number is printed.
        
        Shortcoming: if the substring after the last 
        space is longer than the terminal width, then
        that substring will be wrapped by the terminal,
        and we will not see a newline
        
        :param row_num: the row index number of the row,
            or None if no row number is to precede the row.
        :type row_num: union[int | str | None]
        :param row_str: the row to write, and possibly wrap
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
        
        # Row is longer than terminal is wide:
        # Indent for wrapped lines:
        indent = ' '*row_num_width

        # Find a good line break point:
        is_first_line = True
        while True:
            max_break_pt  = self.term_cols - row_num_width
            # Does the max_break_pt reach just to the end
            # of a column value:
            
            if len(row_str) <= max_break_pt or row_str[max_break_pt] == ' ': 
                good_break_pt = max_break_pt
            else:
                # Find the nearest col break to the left:
                good_break_pt = row_str[:max_break_pt].rfind(' ')
            # If no space found going backwards from the
            # maximal line break point, and nothing follows
            # in row_str after that point, just print the 
            # line, and be done: 
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
        self.cur_page = max(0, self.cur_page - 2)
        self.out_stream.write('\n')
    
    #------------------------------------
    # beginning
    #-------------------
    
    def beginning(self):
        self.cur_page = 0
        self.out_stream.write('\n')        
        
    #------------------------------------
    # end
    #-------------------
    
    def end(self):
        self.cur_page = len(self.pindex) - 1
        self.out_stream.write('\n')        
    
    #------------------------------------
    # getchr
    #-------------------
    
    def getchr(self, prompt=''):
        '''
        If running on a full terminal, such as a Unix/BSD
        terminal window, then read one character from the
        keyboard without echoing, and without requiring
        a NL.
        
        If not running in a full terminal, such as within
        PyCharm or Eclipse, still input a single character,
        but newline is required.
        
        :param prompt: optional prompt to write on same
            line as where input will occur
        :type prompt: str
        :return the character from the keyboard
        :rtype str
        '''

        if not sys.stdin.isatty():
            # Enforce just one char in the 
            # input. The loop can be broken with cnt-c:
            res_str = ''
            while len(res_str) != 1:
                res_str = input(prompt)
            return res_str

        sys.stdout.write(prompt)
        sys.stdout.flush()
        # Get the file descriptor for the terminal
        fd = sys.stdin.fileno()
    
        # remember the initial term mode:
        saved_tty = termios.tcgetattr(fd)
        try:
            # Put the terminal into raw mode
            tty.setcbreak(fd, termios.TCSANOW)
    
            # Read a character from the terminal
            ch = sys.stdin.read(1)
        finally:
            # Restore the terminal to its original mode
            termios.tcsetattr(fd, termios.TCSANOW, saved_tty )

        # Put cursor to start of the prompt line:
        print('\r', end="")
        
        return ch
    