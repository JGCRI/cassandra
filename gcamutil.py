## utility functions used in other gcam python code

## Often we will have to parse values from a config file that are
## meant to indicate a boolean value.  We list here the strings that
## are considered false; everything else is considered true.
def parseTFstring(val):
    falsevals = ["False", "false", "FALSE", "F", "f", "No", "NO", "N", 
                 "no", "0"]
    return val.lstrip().rstrip() not in falsevals
