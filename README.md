## Description
This package contains the following stand-along Unix shell command line tools:

```
- fmore/fless
- ftail
- fwc [-l]
- f2csv
```

Each tool operates in the spirit of the analogous Unix shell commands. Only the most basic uses of these original Unix tools are suported in their *f* version. For example, the `fwc` command operates like `wc -l`, i.e. it displays the number of datalines. But `wc -c` is not provided.

## Installation:
```
- Create virtual environment if desired
- pip install feather-tools
- pip install .
```

For convenience, you might symlink the above files to where they are accessible from different directories. For example, `/usr/local/bin`.

## Usage:

All Python imports are relative to `<proj-root>/src`. So ensure that your $PYTHONPATH includes that directory.

### `fmore` a.k.a. `fless`
```
fmore <fpath>
```
Shows one screenful of the .feather file at a time. The number rows displayed is determined by the terminal in which the command is issued. At the end of each displayed page, type a single character:

- To show the next page: `spacebar`, or `\n`, or the character *n*
- Back one page: *b*
- Back to beginning (page 0): *s*
- To the last page: *e*
- For help: *h*
- To quit the display: *q*

### `ftail <fpath> [(-n | --lines) n]`
Displays the last *n* rows of the .feather file. Default is the lesser of 10, and the length of the data.

**NOTE**: Starts the row display with the logical (i.e. terminal-height) page that contains the first line specified by the tail default or in the `--lines` argument. So a few more rows than
requested may be displayed at the top.

### `fwc <fpath>`
Is analogout to the Unix `wc -l`, and shows the number of data rows, not counting the column header.

### `f2csv <src-fpath> <dst-fpath> [(-s | --separator) <char>]`
Writes a .csv file that contains the .feather data. Default separator is comma.
