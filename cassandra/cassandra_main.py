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
import logging
import os


def bootstrap_sp(args):
    """
    Bootstrap the multithreaded (single processing) version of the calculation.

    :param args: Dictionary of command line arguments parsed by argparse.
    :return: (component-list, capability-table)
    """

    from configobj import ConfigObj
    from cassandra import __version__
    from cassandra.compfactory import create_component

    # Configure logger
    if args['logdir'] is None:
        logging.basicConfig(stream=sys.stdout, level=args['loglvl'])
        logging.info(f'This is Cassandra version {__version__}.')
    else:
        os.makedirs(args['logdir'], exist_ok=True)
        logging.basicConfig(filename=f"{args['logdir']}/cassandra.log",
                            level=args['loglvl'], filemode='w')
        # Write to screen the location of the logging output
        print(f"This is Cassandra version {__version__}.  Output will be logged to {args['logdir']}/cassandra.log")

    cfgfile_name = args['ctlfile']

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


def main(args):
    """
    Cassandra main entry function.

    :param args: Dictionary of command line arguments parsed by argparse.

    This function starts up the Cassandra framework, selecting either the
    single-processing or multi-processing mode, as required.  It's set up to be
    run from the script block at the end of the module, but it could be called
    from another program if desired.  To do that you will need to set up a
    dictionary simulating the command-line parameters from the stand-alone
    version.  The parameters that need to be supplied are:
       ctlfile : Name of the configuration ("control") file.
       mp      : Flag indicating whether we are running in MP mode
       logdir  : Directory for log files. In SP mode this can be None, in 
                 which case log outputs go to stdout
       verbose : Flag indicating whether to produce debugging output.
       quiet   : Flag indicating whether to suppress output except for warnings
                 and error messages
    
    Keep in mind that this function will throw an exception if any of the
    components fail (whether by exception or by returning a failure code).  It's
    up to whatever code is calling it to handle any cleanup associated with the
    failure.  This is especially important if you are running in MP mode, as
    failing to do this cleanup can hang the entire calculation.

    """


    # Set the appropriate logging level
    # NB: You MUST NOT call any logging functions until either bootstrap_mp
    #     or bootstrap_sp has been called.
    if args['verbose']:
        args['loglvl'] = logging.DEBUG
    elif args['quiet']:
        args['loglvl'] = logging.WARNING
    else:
        args['loglvl'] = logging.INFO

    if args['mp']:
        # See notes in mp.py about side effects of importing that module.
        from cassandra.mp import bootstrap_mp, finalize
        (component_list, cap_table) = bootstrap_mp(args)
    else:
        (component_list, cap_table) = bootstrap_sp(args)

    # We will look up "general" in the cap_table and process any
    # global parameters here, but in the current version we don't
    # have any global parameters to process, so skip it.

    threads = []

    for component in component_list:
        logging.info(f"running {str(component.__class__)}")
        threads.append(component.run())

    # Wait for all component threads to complete before printing end
    # message.
    if args['mp']:
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

    if args['mp']:
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
        logging.info('\n****************All components completed successfully.')
    else:
        logging.error(f'\n****************{nfail} components failed.')
        raise RuntimeError(f'{nfail} components failed.')

    # If this is a multiprocessing calculation, then we need to
    # perform the finalization procedure
    if args['mp']:
        finalize(component_list[0], threads[0])

    logging.info("\nFIN.")

    return nfail


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--mp', action='store_true', default=False, help='Use multiprocessing.')
    parser.add_argument('-l', dest='logdir',
                        help='Directory for writing logfiles.  Default = stdout for SP, "logs" for MP')
    parser.add_argument('-v', dest='verbose', action='store_true', default=False,
                        help='Verbose mode: log output at DEBUG level (overrides -q).')
    parser.add_argument('-q', dest='quiet', action='store_true', default=False,
                        help='Quiet mode: log output at WARNING level (overridden by -v).')
    parser.add_argument('ctlfile', help='Name of the configuration file for the calculation.')

    argvals = parser.parse_args()

    try:
        # status is the number of component failures.
        status = main(vars(argvals))
    except Exception as err:
        if argvals.mp:
            from mpi4py import MPI
            from logging import exception
            exception('Fatal error:  calling MPI_Abort.')
            MPI.COMM_WORLD.Abort()
        raise

    # end of main block.
