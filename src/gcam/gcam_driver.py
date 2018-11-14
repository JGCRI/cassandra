#!/bin/env python
"""GCAM driver

  usage:  python gcam_driver.py <configfile>

  This program will run the GCAM automation system using the configuration details
  from the configuration file described on the command line.  The configuration
  file format and contents are described in the GCAM Automation Users' Guide.

"""
from configobj import ConfigObj


def gcam_parse(cfgfile_name):
    """Parse the configuration file for the GCAM driver."""

    # initialize the structures that will receive the data we are
    # parsing from the file
    capability_table = {}
    module_list = []

    # cfgfile_name is a filename
    config = ConfigObj(cfgfile_name)

    try:
        global_config = config["Global"]
        mod_table_file = global_config["modtable"]
    except KeyError:
        print("Config file must have a '[Global]' section containing a 'modtable' key")
        raise

    # Only create modules that are in the master module table file
    with open(mod_table_file, "r") as mod_table:
        next(mod_table)  # Skip header line

        for line in mod_table:
            mod_name, mod_config_section = [x.strip() for x in line.split(',')]
            mod_create = "{}(capability_table)".format(mod_name)
            print("Module init statement: {}\n".format(mod_create))

            module = eval(mod_create)
            module.params.update(config[mod_config_section])
            module.finalize_parsing()
            module_list.append(module)

    return module_list, capability_table


if __name__ == "__main__":
    import sys
    import os

    # arrange so that when run from the top-level directory we still find
    # the modules we want to load.
    sys.path.append(os.getcwd() + '/src')
    from gcam.modules import *

    try:
        (modlist, cap_table) = gcam_parse(sys.argv[1])
    except IndexError:
        print(__doc__)
        sys.exit(0)

    # We will look up "global" in the cap_table and process any
    # global parameters here, but in the current version we don't
    # have any global parameters to process, so skip it.

    threads = []

    for module in modlist:
        print("running {}".format(module.__class__))
        threads.append(module.run())

    # Wait for all threads to complete before printing end message.
    for thread in threads:
        thread.join()

    # Check to see if any of the modules failed
    fail = 0
    for module in modlist:
        if module.status != 1:
            print("Module {} returned failure status\n".format(module.__class__))
            fail += 1

    if fail == 0:
        print("\n****************All modules completed successfully.")
    else:
        print("\n****************{} modules failed.".format(fail))

    print("\nFIN.")
