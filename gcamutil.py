import os
import re
import subprocess

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
    return pat.sub(line, newstr)

### Run the indicated queries against a dbxml database
###  dbxml:           the dbxml file to query
###  queryfiles:      list of xml files containing the batch queries to run.  If
###                   there is only one, you can just pass the filename
def gcam_query(dbxml, queryfiles):
    if hasattr(queryfiles,'__iter__'):
        qlist = queryfiles
    else:
        qlist = [queryfiles]

    ModelInterface = genparams["ModelInterface"]
    DBXMLlib       = genparams["DBXMLlib"]
        
    ## start up the virtual frame buffer.  The Model Interface needs
    ## this even though it won't be displaying anything.
    xvfb = subprocess.Popen(['Xvfb', ':1', '-pn', '-audit', '4', '-screen', '0', '800x600x16'])
    ldlibpath = os.getenv('LD_LIBRARY_PATH')
    if ldlibpath is None:
        ldlibpath = "LD_LIBRARY_PATH=%s"%DBXMLlib
    else:
        ldlibpath = "LD_LIBRARY_PATH=%s:%s" % (ldlibpath,DBXMLlib) 
        
    for query in qlist:
        execlist = ['/bin/env', 'DISPLAY=:1.0', ldlibpath, 'java', '-jar',
                    ModelInterface, '-b', query]

        subprocess.call(execlist)

    xvfb.kill()

    ## output from these queries goes into csv files.  The names of
    ## these files are in the query file, so it's up to the caller to
    ## know or figure out where its data will be.
    return                      # no return value.
