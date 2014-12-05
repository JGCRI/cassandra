#!/bin/env python
import re
from gcam_modules import *

## usage: gcam_driver.py <configfile> 

def gcam_parse(cfgfile_name):
    ## initialize the structures that will receive the data we are
    ## parsing from the file
    capability_table = {}
    module_list      = []

    ## cfgfile_name is a filename
    with open(cfgfile_name,"r") as cfgfile: 
        section   = None
        module    = None
        sectnpat  = re.compile(r'\[(.+)\]')
        keyvalpat = re.compile(r'(.+)=(.+)')

        for line in cfgfile:
            line = line.lstrip()        # remove leading whitespace

            ## check for comments and blank lines.  A line is a comment if
            ## the first non-whitespace character is a '#'
            if(line == "" or line[0] == '#'):
                continue

            ## check for section header.  Section headers appear in square brackets:  [gcam_module]
            sectnmatch = sectnpat.match(line)
            if sectnmatch:
                section = sectnmatch.group(1)
                print "parser starting section:  %s" % section
                
                if not section.lower()=="global":
                    ## Section header starts a new module
                    ## create the new module:  the section name is the module class
                    ## TODO: is the input from the config file trusted enough to do it this way?
                    modcreate = "%s(capability_table)" % section
                    print "modcreate statement:  %s\n" % modcreate
                    module = eval(modcreate)
                else:
                    ## This is kind of a wart because I want to call
                    ## the section "global", but I don't want to have
                    ## a module called "global".
                    module = GlobalParamsModule(capability_table)
                    
                module_list.append(module) 
                continue        # nothing further to do for a section header line

            ## If we get this far, we have a nonblank line that is not a
            ## comment or a section header.  We had better be in a section
            ## by now, or the config is malformed.
            if section==None:
                raise RuntimeError("Malformed config file:  doesn't open with a section header.")

            kvmatch = keyvalpat.match(line)
            if not kvmatch:
                raise RuntimeError("Malformed line in config file:\n%s"%line)

            key = kvmatch.group(1).lstrip().rstrip()
            val = kvmatch.group(2).lstrip().rstrip()

            print "parser got key= %s\tval= %s" % (key, val)

            module.addparam(key, val)

        ## end of loop over config file lines
    ## end of with block:  config file will be closed
            
    ## close out the parameter processing for all modules in the list
    for module in module_list:
        module.finalize_parsing()

    return (module_list, capability_table)
## end of gcam_parse

if __name__ == "__main__":
    from sys import argv

    (modlist, cap_table) = gcam_parse(argv[1])

    ## We will look up "global" in the cap_table and process any
    ## global parameters here, but in the current version we don't
    ## have any global parameters to process, so skip it.

    for module in modlist:
        print "running %s" % module.__class__
        module.run()

    print "FIN."
    
