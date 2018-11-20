#!/usr/bin/env python3
"""Cassandra model coupling framework

  usage:  cassandra_main.py <configfile>

  This program will run the cassandra model coupling system using the
  configuration details from the configuration file supplied on the
  command line.  The configuration file format and contents are
  described in the Cassandra Users' Guide.

"""

import sys
import re
import threading
import argparse


def bootstrap_sp(cfgfile_name):
    """
    Bootstrap the multithreaded (single processing) version of the
    calculation.

    :param cfgfile_name:  Name of the configuration file
    :return: (component-list, capability-table)
    """

    from configobj import ConfigObj
    from cassandra.compfactory import create_component

    # initialize the structures that will receive the data we are
    # parsing from the file
    capability_table = {}
    component_list = []

    # cfgfile_name is a filename
    # cfgfile_name is a filename
    config = ConfigObj(cfgfile_name)

    try:
        global_config = config["Global"]
    except KeyError:
        raise RuntimeError("Config file must have a '[Global]' section")

    for section in config.keys():
        component = create_component(section, capability_table)
        component.params.update(config[section])
        component.finalize_parsing()
        component_list.append(component)

    return (component_list, capability_table)

# end of bootstrap_sp


if __name__ == "__main__":
    from cassandra.components import *

    parser = argparse.ArgumentParser()
    parser.add_argument('--mp', action='store_true', default=False, help='Use multiprocessing.')
    parser.add_argument('ctlfile', help='Name of the configuration file for the calculation.')

    argvals = parser.parse_args()

    if argvals.mp:
        raise NotImplementedError('Multiprocessing is not yet implemented.')

    try:
        (component_list, cap_table) = bootstrap_sp(argvals.ctlfile)
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
