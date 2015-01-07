import os
import re
import subprocess
import tempfile

## utility functions used in other gcam python code

## Place holder for the general params structure.  The constructor for
## that structure knows it's supposed to add itself here.
genparams = None

## Often we will have to parse values from a config file that are
## meant to indicate a boolean value.  We list here the strings that
## are considered false; everything else is considered true.
def parseTFstring(val):
    falsevals = ["False", "false", "FALSE", "F", "f", "No", "NO", "N", 
                 "no", "0"]
    return val.lstrip().rstrip() not in falsevals


### CSV files returned from the model interface frequently have a
### scenario name as the first field.  The scenario name invariably has
### a comma in it, which really messes up splitting on commas.  We
### almost never use the scenario name for anything, so it's best to
### transform it to something benign.  That's what this next function
### does.
## Regular expression for detecting a scenario name
scen_pattern = re.compile(r'^"[^"]*"') # Beginning of line, followed by a ", followed
                                       # by any number of non-" chars, followed by a "

def scenariofix(line, newstr="scenario", pat=scen_pattern):
    ## newstr is the string to substitute in place of the quoted scenario field
    ## pat is the pattern to use for detecting the scenario field.  It should
    ## be very rare to need to change it.
    return pat.sub(newstr, line)

### Run the indicated queries against a dbxml database
###  queryfiles:      list of xml files containing the batch queries to run.  If
###                   there is only one, you can just pass the filename
###  dbxmlfiles:      list of dbxml file or files to query.  If there is only
###                   one, you can just pass the filename.  If there
###                   is a list of query files and only a single
###                   dbxml, the queries will all be run against the
###                   same dbxml.
### outfiles:         list of output files.  should be the same length as
###                   the query list.
def gcam_query(queryfiles, dbxmlfiles, outfiles):
    if hasattr(queryfiles,'__iter__'):
        qlist = queryfiles
    else:
        qlist = [queryfiles]

    if hasattr(dbxmlfiles, '__iter__'):
        dbxmllist = dbxmlfiles
        if len(dbxmllist) == 1:
            dbxmllist = dbxmllist*len(qlist)
    else:
        dbxmllist = [dbxmlfiles]*len(qlist)

    if hasattr(outfiles, '__iter__'):
        outlist = outfiles
    else:
        outlist = [outfiles]

    ## check for agreement in lengths of the above lists
    if len(dbxmllist) != len(qlist) or len(outlist) != len(qlist):
        raise RuntimeError("Mismatch in input lengths for gcam_query.") 

    ModelInterface = genparams["ModelInterface"]
    DBXMLlib       = genparams["DBXMLlib"]
        
    ## start up the virtual frame buffer.  The Model Interface needs
    ## this even though it won't be displaying anything.
    xvfb = subprocess.Popen(['Xvfb', ':1', '-pn', '-audit', '4', '-screen', '0', '800x600x16'])
    try:
        ldlibpath = os.getenv('LD_LIBRARY_PATH')
        if ldlibpath is None:
            ldlibpath = "LD_LIBRARY_PATH=%s"%DBXMLlib
        else:
            ldlibpath = "LD_LIBRARY_PATH=%s:%s" % (ldlibpath,DBXMLlib) 

        for (query, dbxml, output) in zip(qlist,dbxmllist,outlist):
            print query, output
            ## make a temporary file
            tempquery = None
            try:
                tempquery = rewrite_query(query, dbxml, output)
                execlist = ['/bin/env', 'DISPLAY=:1.0', ldlibpath, 'java', '-jar',
                            ModelInterface, '-b', tempquery]

                subprocess.call(execlist)

            finally:
                if tempquery:
                    os.unlink(tempquery)
    finally:
        xvfb.kill()

    ## output from these queries goes into csv files.  The names of
    ## these files are in the query file, so it's up to the caller to
    ## know or figure out where its data will be.
    return outlist              # probably redundant, since the list of output files was an argument.

## The name of the input dbxml file is encoded in the query file.
## Since we want to be able to set it, we need to treat the query file
## as a template and create a temporary with the real file name.  This
## function creates the temporary and returns its name.

### Some regular expressions used in query_file_rewrite:
xmldbloc   = re.compile(r'<xmldbLocation>.*</xmldbLocation>')
outfileloc = re.compile(r'<outFile>.*</outFile>')
def rewrite_query(query, dbxml, outfile):
    (fd, tempqueryname) = tempfile.mkstemp(suffix='.xml') 

    ## copy the input query file line by line into the temp
    ## file; however, edit the xmldb and output locations to
    ## match the arguments.
    origquery = open(query,"r")
    tempquery = os.fdopen(fd,"w")

    dbxmlstr = '<xmldbLocation>' + dbxml + '</xmldbLocation>'
    outfilestr = '<outFile>' + outfile + '</outFile>'
    
    for line in origquery:
        line = xmldbloc.sub(dbxmlstr, line)
        line = outfileloc.sub(outfilestr, line)
        tempquery.write(line)

    tempquery.close()
    return tempqueryname

        
## regex for removing trailing commas
trlcomma = re.compile(r',\s*$')
def rm_trailing_comma(line):
    return trlcomma.sub('',line)
