## Description
Stand-alone Unix shell command line tools for Apache's .feather formatted files:

```
- fless
- ftail
- fwc (-l)
- f2csv
```

Each tool operates in the spirit of the analogous Unix shell command. Only the most basic uses of these original Unix tools are suported in their *f* version. For example, the `fwc` command operates like `wc -l`, i.e. it displays the number of data rows. But `wc -c` is not provided.

## Installation:
```
- Create virtual environment if desired
- pip install feather-tools
- pip install .
```
The files will be in .../site-packages/feather_tools. For convenience, you might add that location to $PATH.

## Usage:

### `fless`
```
fless <fpath>
```
Shows one screenful of the .feather file at a time. The number of rows displayed is determined by the terminal in which the command is issued. At the end of each displayed page, type a single character:

- To show the next page: `spacebar`, or `\n`, or the character *n*
- Back one page: *b*
- Back to beginning (page 0): *s*
- To the last page: *e*
- For help: *h*
- To quit the display: *q*

### `ftail <fpath> [(-n | --lines) n]`
Displays the last *n* rows of the .feather file. Default is the lesser of 10, and the length of the data.

**NOTE**: Starts the row display with the logical (i.e. terminal-height) page that contains the first line specified by the tail default or in the `--lines` argument. So a few more rows than requested may be displayed at the top.

### `fwc <fpath>`
Is analogout to the Unix `wc -l`, and shows the number of data rows, not counting the column header.

### `f2csv <src-fpath> [<dst-fpath>] `
Writes a .csv file that contains the .feather data. Default separator is comma. Default output is stdout.

The command line arguments are as in the [Pandas `df.to_csv()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html) documentation. This means that the `index` keyword is True by default, while `index_label` is None. Which leads to ugly .csv like:
```
,foo,bar,fum
Row0,1,2,3
Row1,4,5,6
Row2,7,8,9
```
Where the `Row`*n* are the dataframe index. Note the orphan comma in the header. To fix (same as with df.to_csv()):
```
f2csv --index=true --index_label=Rows file.feather

Rows,foo,bar,fum
Row0,1,2,3
Row1,4,5,6
Row2,7,8,9
```
or:
```
f2csv --index=false file.feather

foo,bar,fum
1,2,3
4,5,6
7,8,9
```
If you clone this repo to make changes: all Python imports are relative to `<proj-root>/src`. So ensure that your $PYTHONPATH includes that directory.

## Testing
Running nose2 in the project root runs the tests. One abnormality: in order to blend with Unix convention, the command files are without a '.py' extension. Examples: `fless`, `fwc`. The downside is that unittest files cannot load Python files without .py extensions. Copies of the command files are therefore placed in the test subdirectory. The unittests run on those copies. If changes are made to the command files, then those copies must be updated before testing. Symlinks are not an option, because pip cannot recreate them during installation.
