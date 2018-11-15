#!/bin/env python
"""Cassandra model coupling framework

  usage:  cassandra.py <configfile>

  This program will run the cassandra model coupling system using the
  configuration details from the configuration file supplied on the
  command line.  The configuration file format and contents are
  described in the Cassandra Users' Guide.

"""

import sys
import re
import threading


def config_parse(cfgfile_name):
    """Parse the configuration file."""

    # initialize the structures that will receive the data we are
    # parsing from the file
    capability_table = {}
    component_list = []

    # cfgfile_name is a filename
    with open(cfgfile_name, "r") as cfgfile:
        section = None
        component = None
        sectnpat = re.compile(r'\[(.+)\]')
        keyvalpat = re.compile(r'(.+)=(.+)')

        for line in cfgfile:
            line = line.lstrip()        # remove leading whitespace

            # check for comments and blank lines.  A line is a comment if
            # the first non-whitespace character is a '#'
            if(line == "" or line[0] == '#'):
                continue

            # check for section header.  Section headers appear in square brackets. e.g.: [gcam_component]
            sectnmatch = sectnpat.match(line)
            if sectnmatch:
                section = sectnmatch.group(1)
                print(f"parser starting section:  {section}")

                if not section.lower() == "global":
                    # Section header starts a new component
                    # create the new component:  the section name is the component class
                    # TODO: is the input from the config file trusted enough to do it this way?
                    component_create = f"{section}(capability_table)"
                    print(f"component_create statement:  {component_create}\n")
                    component = eval(component_create)
                else:
                    # This is kind of a wart because I want to call
                    # the section "global", but I don't want to have
                    # a component called "global".
                    component = GlobalParamsComponent(capability_table)

                component_list.append(component)
                continue        # nothing further to do for a section header line

            # If we get this far, we have a nonblank line that is not a
            # comment or a section header.  We had better be in a section
            # by now, or the config is malformed.
            if section == None:
                raise RuntimeError("Malformed config file:  doesn't open with a section header.")

            kvmatch = keyvalpat.match(line)
            if not kvmatch:
                raise RuntimeError(f"Malformed line in config file:\n{line}")

            key = kvmatch.group(1).lstrip().rstrip()
            val = kvmatch.group(2).lstrip().rstrip()

            print(f"parser got key= {key}\tval= {val}")

            component.addparam(key, val)

        # end of loop over config file lines
    # end of with block:  config file will be closed

    # close out the parameter processing for all components in the list
    for component in component_list:
        component.finalize_parsing()

    return (component_list, capability_table)
# end of config_parse


if __name__ == "__main__":
    from cassandra.components import *

    try:
        (component_list, cap_table) = config_parse(sys.argv[1])
    except IndexError:
        print(__doc__)
        sys.exit(0)

    # We will look up "global" in the cap_table and process any
    # global parameters here, but in the current version we don't
    # have any global parameters to process, so skip it.

    threads = []

    for component in component_list:
        print(f"running {str(component.__class__)}")
        threads.append(component.run())

    # Wait for all threads to complete before printing end message.
    for thread in threads:
        thread.join()

    # Check to see if any of the components failed
    fail = 0
    for component in component_list:
        if component.status != 1:
            print(f'Component {str(component.__class__)} returned failure status\n')
            fail += 1

    if fail == 0:
        print('\n****************All components completed successfully.')
    else:
        print(f'\n****************{fail} components failed.')

    print("\nFIN.")
