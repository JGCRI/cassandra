import re
## utility functions used in other gcam python code

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

