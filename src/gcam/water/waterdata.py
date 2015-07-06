#!/bin/env python
"""Functions for reformatting the data produced by the water code."""

import re
import sys
import os

country_watercode_table = None
## XXX fixme
default_water_iso_file = './input-data/water-country-ISO.csv'

def read_matlab_csv(filename, idcol=True):
    """Read a csv file produced by Matlab.

    Arguments -
        filename - Name of the input file
        idcol    - If True, the first column will be treated as an
                   integer identifier.  (DEFAULT = True)

    Return value -
        table read from the file, represented as a list of lists.
    """

    table = []
    with open(filename,"r") as file:
        for line in file:
            fields = line.split(',');
            if idcol:
                fields = [int(fields[0])] + map(float, fields[1:])
            else:
                fields = map(float, fields)
            table.append(fields)

    return table

def watercode2iso(table, idcol=True, start=1):
    """Translate the nonstandard numerical codes used to identify countries in the water code to ISO codes.

    Arguments -
        table   - The data table to be translated
        
        idcol   - If True, then the first column is a column of id
                  numbers, to be replaced with ISO codes.  Otherwise
                  the country id will be assumed equal to the row
                  index, and the iso codes will be added as a new
                  column at the left.  (DEFAULT = True)
                  
         start  - ID number in the table of the first country in the
                  master list (this country may or may not actually be
                  present in the data).  Natively this is zero, but
                  the matlab code frequently has to offset this to
                  ensure that aggregation functions work properly.
                  This argument is ignored if idcol is False.
                  (DEFAULT = 1)

    Return value - 
        Revised table with the ID numbers replaced with ISO codes, or
        the ISO codes added, depending on the setting of the idcol
        argument.  The table will be sorted by ISO code.
    """

    if country_watercode_table is None:
        read_watercode_table()

    if idcol:
        ## subtract off the start value, so that the country indices
        ## run from 0 - 248.
        ctrycodes = map(lambda row: row[0]-start, table) 
    else:
        ## generate a set of country codes from the row indices, and
        ## add a dummy column to fill in with the ISO code.
        ctrycodes = range(len(table))
        table     = map(lambda row: [0]+row, table)

    badcodes = []               # There are some country codes used by
                                # the hydro code that we don't use.
    for idx in range(len(table)):
        ccode = ctrycodes[idx]
        if ccode in country_watercode_table:
            table[idx][0] = country_watercode_table[ccode]
        else:
            badcodes.append(idx)

    badcodes.reverse()          # so that the indices remain valid as each element is removed.
    for badidx in badcodes:
        sys.stdout.write('Removing bad country code: %s\n'%str(ctrycodes[badidx]))
        table.pop(badidx)

    table.sort()
    return table

def read_watercode_table(filename=default_water_iso_file):
    """Read the table translating country IDs to ISOs for the ID scheme used in the water code.

    This function will be called automatically with a default filename
    the first time a function tries to read the table.  It only needs
    to be called explicitly if you want to use an alternate table for
    some reason.
    """

    ## We're going to make this a dictionary because we can't be
    ## completely sure that the codes form a contiguous range of
    ## numbers (they do now, but that could change; e.g., if we
    ## replaced them with the ISO numerical codes.
    global country_watercode_table
    country_watercode_table = {}
    try:
        with open(filename,'r') as infile:
            infile.readline()       # skip header line
            for line in infile:
                data = line.split(',')
                code = int(data[0])
                country_watercode_table[code] = data[1]
    except IOError:
        sys.stderr.write("From directory: %s\n\tCan't open file: %s\n" % (os.getcwd(),filename))
        raise
##end of fn.

def write_csv(filename, table, header=None):
    """Write a table in list-of-lists format to a comma-separated value file.

    In order to avoid having to deal with quote delimiters we just
    replace any commas embedded in the output with underscores.
    """

    ## function to scrub (some) bad formatting from data items
    scrub = lambda x: str(x).replace(',','_').replace('\n','')
    with open(filename,"w") as outfile:
        if header is not None:
            hdrscrub = map(scrub, header)
            outfile.write(', '.join(hdrscrub))
            outfile.write('\n')
        for row in table:
            rowscrub = map(scrub, row)
            outfile.write(', '.join(rowscrub))
            outfile.write('\n')
##end of fn

            
        
    
