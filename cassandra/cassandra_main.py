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


def main(argvals):
    if argvals.mp:
        # See notes in mp.py about side effects of importing that module.
        from cassandra.mp import bootstrap_mp, finalize
        (component_list, cap_table) = bootstrap_mp(argvals)
    else:
        (component_list, cap_table) = bootstrap_sp(argvals.ctlfile)

    # We will look up "general" in the cap_table and process any
    # global parameters here, but in the current version we don't
    # have any global parameters to process, so skip it.

    threads = []

    for component in component_list:
        print(f"running {str(component.__class__)}")
        threads.append(component.run())

    # Wait for all component threads to complete before printing end
    # message.
    if argvals.mp:
        # The RAB thread will always be first in the thread list
        component_threads = threads[1:]
    else:
        # No RAB in a single-node calculation
        component_threads = threads

    for thread in component_threads:
        thread.join()

    # Check to see if any of the components failed, and that the RAB
    # is still running.  Once again take advantage of the fact that
    # if the RAB is present, it is always the first in the list.
    nfail = 0
    
    if argvals.mp:
        reg_comps = component_list[1:]
        rab_comp = component_list[0]
    else:
        reg_comps = component_list
        rab_comp = None
        
    for component in reg_comps:
        if component.status != 1:
            from logging import error
            error(f'Component {str(component.__class__)} returned failure status\n')
            nfail += 1
            
    if rab_comp is not None and rab_comp.status != 0:
        from logging import error
        error('RAB has crashed or is otherwise not running.')
        nfail += 1
        

    if nfail == 0:
        print('\n****************All components completed successfully.')
    else:
        print(f'\n****************{nfail} components failed.')
        raise RuntimeError(f'{nfail} components failed.')

    # If this is a multiprocessing calculation, then we need to
    # perform the finalization procedure
    if argvals.mp:
        finalize(component_list[0], threads[0])

    print("\nFIN.")

    return nfail


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--mp', action='store_true', default=False, help='Use multiprocessing.')
    parser.add_argument('ctlfile', help='Name of the configuration file for the calculation.')

    argvals = parser.parse_args()

    try:
        # status is the number of component failures.
        status = main(argvals)
    except Exception as err:
        if argvals.mp:
            from mpi4py import MPI
            from logging import exception
            exception('Fatal error:  calling MPI_Abort.')
            MPI.COMM_WORLD.Abort()
        raise

    # end of main block.
