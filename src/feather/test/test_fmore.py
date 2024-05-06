'''
Created on May 1, 2024

@author: paepcke
'''
from feather.fmore import Pager
#from feather.fmore import FMore
import io
import numpy as np
import pandas as pd
import tempfile
import unittest



TEST_ALL = True
#TEST_ALL = False

class FMoreTester(unittest.TestCase):


    def setUp(self):
        self.create_test_files()


    def tearDown(self):
        try:
            self.tmpdir.cleanup()
        except Exception:
            pass

    # ----------------------- Tests -------------
    
    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_pagination_cache(self):
        
        term_lines = 1 
        pager = Pager(self.df_narrow_and_short, term_lines, unittesting=True)
        df = self.df_narrow_and_short
        data_lines_per_page = 2
        num_col_lines = 1
        
        pindex = pager._pagination_index(df, data_lines_per_page, num_col_lines)
        
        expected = {
            0  : (0,0),
            1  : (0,2),
            2  : (2,4),
            3  : (4,6),
            } 
        self.assertDictEqual(pindex, expected)

        # Everything fits on one page:
        data_lines_per_page = 30
        num_col_lines = 1
        term_cols = 50
        term_lines = 100
        pager = Pager(self.df_narrow_and_short, term_lines, term_cols=term_cols, unittesting=True)
        pindex = pager._pagination_index(df, data_lines_per_page, num_col_lines)
        expected = {
              0 : (0,6)
            }
        self.assertDictEqual(pindex, expected)
     
    #------------------------------------
    # test__compute_line_overflow_safety 
    #-------------------
        
    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test__compute_line_overflow_safety(self):
        
        # No overflow needed:
        
        df = self.df_narrow_and_short
        term_cols  = 80
        term_lines = 50
        pager = Pager(df, term_lines, term_cols)
        
        num_col_lines, lines_per_page = pager._compute_lines_per_page(df)
        
        self.assertEqual(num_col_lines, 1)
        self.assertEqual(lines_per_page, term_lines)

        # Column header is larger than term width:
        # Make column names 10 chars each
        df.columns = ['1234567890', '1234567890', '1234567890']
        term_cols  = 25
        col_extras, lines_per_page = pager._compute_lines_per_page(df)
        self.assertEqual(col_extras, 1)
        self.assertEqual(lines_per_page, term_lines)

        # Wide data rows:
        df = self.df_wide_and_long
        
        # Width of col names:589
        #header_width = len(df.columns)
        # Data: 100 wide:
        #data_width = len(df.iloc[2])
        # 3072 rows:
        #num_rows   = len(df)
        term_cols  = 25
        term_lines = 10
        pager = Pager(df, term_lines, term_cols)

        col_lines, lines_per_page = pager._compute_lines_per_page(df)
        
        self.assertEqual(col_lines, 34)
        self.assertEqual(lines_per_page, 1)
        
        # Higher terminal:
        term_lines = 100
        
        pager = Pager(df, term_lines, term_cols)
        col_lines, lines_per_page = pager._compute_lines_per_page(df)
        
        self.assertEqual(col_lines, 34)
        self.assertEqual(lines_per_page, 2)
        
    #------------------------------------
    # test_paging
    #-------------------
    
    # @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    # def test_paging(self):
    #
    #     #_fmore = FMore(self.path_narrow_and_short.name)
    #     _fmore = FMore(self.path_wide_and_long.name, lines=38, cols=111)
    #     #_fmore = FMore(self.path_narrow_and_short.name)
    #     #_fmore = FMore(self.path_wide_and_short.name)


    #------------------------------------
    # test__num_broken_lines
    #-------------------
    
    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test__num_broken_lines(self):
        
        term_cols  = 10
        _term_lines = 30
        df = pd.DataFrame()
        pager = Pager(df, _term_lines, term_cols, unittesting=True)
        
        # Should fit in one row:
        test_str = 'foo'
        num_lines = pager._num_wrapped_lines(None, test_str)
        self.assertEqual(num_lines, 1)
    
        # Empty string:
        test_str = ''
        num_lines = pager._num_wrapped_lines(None, test_str)
        self.assertEqual(num_lines, 1)
        
        test_str = '1234567890 Next line'
        num_lines = pager._num_wrapped_lines(None, test_str)
        self.assertEqual(num_lines, 2)

        # Last line longer than terminal, but no
        # space for wrapping:        
        test_str = '1234567890 Next linewithoutanybreakforwrapping'
        num_lines = pager._num_wrapped_lines(None, test_str)
        self.assertEqual(num_lines, 3)
        

    #------------------------------------
    # test__write_tab_row
    #-------------------
    
    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test__write_tab_row(self):
        
        term_lines = 30
        row_num = 1
        row_str = "column1    column2    10    1000"
        # Df doesn't matter:
        df = pd.DataFrame()
        
        # Just enough to include row num of '1:' plus up to 'columns2'
        term_cols  = 21
        buf = io.StringIO()
        pager = Pager(df, term_lines, term_cols=term_cols, out_stream=buf, unittesting=True)
        
        pager._write_tab_row(row_num, row_str)
        
        row_printed = buf.getvalue()
        expected = '1: column1    column2\n   10    1000\n'
        self.assertEqual(row_printed, expected)

    #------------------------------------
    # test__estimate_col_print_width
    #-------------------
    
    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test__estimate_col_print_width(self):
        
        wide_str = 'Very wide value'
        df = pd.DataFrame(
            {'Narrow' : 10,
             'Wide'   : wide_str
             }, index=[0,1])
        pager = Pager(df, 80, 35)
        width = pager._estimate_col_print_width(df, padding=0)
        self.assertEqual(width, len(wide_str))

        # Corner case: empty df:
        width = pager._estimate_col_print_width(pd.DataFrame(), padding=0)
        self.assertEqual(width, 0)

    #------------------------------------
    # test_getchr
    #-------------------

    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_getchr(self):
        '''
        This test involves a user typing a character,
        and reading it without the user having to type
        a newline. 
        
        In order not to hang automatic tests, the central
        section that includes:
        
               pager.getchr()
               
        is commented out. Uncommenting, and running in a terminal
        window will echo a character that the user types.
        
        NOTE: the Eclipse/PyCharm console views are not real
              terminals. So the getchr() facility will not work
              there.
        '''
        _df = self.df_narrow_and_short
        _term_cols  = 80
        _term_lines = 50

        # UNCOMMENT for manual test in a terminal window:
        
        # pager = Pager(_df, _term_cols, _term_lines)
        # ch = pager.getchr("Type a char: ")
        # print(f"\nYou typed: '{ch}'")
        
    # ----------------------- Utilities --------------
    
    def create_test_files(self):
        
        self.tmpdir = tempfile.TemporaryDirectory(dir='/tmp', prefix='fmore_')
        
        self.df_narrow_and_short = pd.DataFrame(
            {'foo' : [10,20,30,40,50,60],
             'bar' : [100,200,300,400,500,600],
             'fum' : [1000,2000,3000,4000,5000,6000]
            })

        cols = [f"Col{num}" for num in list(range(100))]
        idx  = ['Row0', 'Row1', 'Row2']
        self.df_wide_and_short = pd.DataFrame(
            np.array([
                np.array(list(range(100))),
                np.array(list(range(100))) + 100,
                np.array(list(range(100))) + 200
                ]), 
            columns = cols, index=idx 
            )
        
        arr_wide_and_long = self.df_wide_and_short.values 
        for _ in list(range(10)):
            arr_wide_and_long = np.vstack((arr_wide_and_long, arr_wide_and_long))
        
        self.df_wide_and_long = pd.DataFrame(arr_wide_and_long, columns=self.df_wide_and_short.columns)
        
        self.path_narrow_and_short = tempfile.NamedTemporaryFile( 
                                                                 suffix='.feather', 
                                                                 prefix="fmore_", 
                                                                 dir=self.tmpdir.name, 
                                                                 delete=False)
        self.path_wide_and_short = tempfile.NamedTemporaryFile(
                                                               suffix='.feather', 
                                                               prefix="fmore_", 
                                                               dir=self.tmpdir.name, 
                                                               delete=False)
        self.path_wide_and_long = tempfile.NamedTemporaryFile(
                                                              suffix='.feather', 
                                                              prefix="fmore_", 
                                                              dir=self.tmpdir.name, 
                                                              delete=False)

        
        self.df_narrow_and_short.to_feather(self.path_narrow_and_short)
        self.df_wide_and_short.to_feather(self.path_wide_and_short)
        self.df_wide_and_long.to_feather(self.path_wide_and_long)
        
        self.path_narrow_and_short.close()
        self.path_wide_and_short.close()
        self.path_wide_and_long.close()
        
        
# ----------------------- Main --------------        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()