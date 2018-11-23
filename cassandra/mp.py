"""Multiprocessing support for cassandra

Multiprocessing in this context means running on multiple, independent hosts,
such as the compute nodes in a cluster.  Running on a single host does not
require this module; that case is handled using multithreading in the vanilla
framework.

Importing this module will also import mpi4py, which in turn will attempt to
call MPI_Init_thread().  This is likely to fail spectacularly if the code wasn't
run with mpirun.  Conversely, running with mpirun and failing to supply the flag
that activates this module will also not have the effect you want.  Therefore,
you must either run:
    cassandra_main.py <configfile>
or
    mpirun cassandra_main.py --mp <configfile>
It would be nice if there were a way to detect automatically whether the system
was being run under MPI or not, but there is no general way to do this, so it's
up to users to make certain that their startup procedure is consistent.

Finally, Cassandra's control scheme makes extensive use of multithreading, with
threads blocking whenever the data they need is unavailable; therefore, it will
only be possible to run the framework under an MPI implementation that supports
the MPI_THREAD_MULTIPLE threading level.  If in doubt, always check your MPI
installation for compatibility.  Unfortunately, each MPI distribution has a
different way of doing this, but the mpi-thread-test.c program in the extras
directory should always work.

"""

from mpi4py import MPI
from cassandra.rab import RAB
from cassandra.constants import TAG_CONFIG, SUPERVISOR_RANK
from cassandra.compfactory import create_component
import logging


def bootstrap_mp(argvals):
    """Bootstrap the multiprocessing system.

    1. Parse configuration files
    2. Distribute configuration information to peers
    3. Create RABs
    4. Create components
    5. Add remote capabilities to capability table
    6. Return component list (including RAB).  The main loop will be able to
    start these components in the usual way.
    """

    world = MPI.COMM_WORLD
    rank = world.Get_rank()

    logging.basicConfig(filename=f'logs/cassandra-{rank}.log', level=logging.DEBUG)
    
    if rank == SUPERVISOR_RANK:
        my_assignment = distribute_assignments_supervisor(argvals)
    else:
        my_assignment = distribute_assignments_worker(argvals)


    # my_assignment will be a dictionary of configuration sections assigned to
    # this process.  We need to create and initialize the components assigned to
    # us.  We also need to create a RAB.
    cap_tbl = {}
    rab = RAB(cap_tbl, world)
    comps = [rab]
    logging.debug(f'rank: {rank} assignments: {my_assignment}\n')
    for section, conf in my_assignment.items():
        component = create_component(section, cap_tbl)
        component.params.update(conf)
        component.finalize_parsing()
        comps.append(component) 

    # Next we need to compile a table of remote capabilities.  To do this, each
    # component needs to distribute its local capability table to all the other
    # ranks, which we can do with an allgather collective.  Then each rank adds
    # all of the capabilities it got from other ranks to its RAB's list of
    # capabilities.  (We don't actually send the whole capability table, just
    # the names of the capabilities it contains.)
    capabilities = list(cap_tbl.keys())
    capabilities.remove('general')
    logging.debug(f'rank {rank} capabilities:  {list(cap_tbl.keys())}')
    allcaptbls = world.allgather(capabilities)

    for i, remote_cap in enumerate(allcaptbls):
        if i == rank:
            # skip our own contribution to the list
            continue

        rab.addremote(i, remote_cap)

    # Return the component list and capability table
    return (comps, cap_tbl)


def distribute_assignments_worker(argvals):
    """Prepare to receive component assignments

    :param argvals: Arguments structure parsed by argparse.  Not currently used,
                    but included in case we eventually want to have command line
                    arguments that affect the way assignments are handled.
    :return: Dictionary of component definitions for components assigned to this
                    worker.

    This function should be called by the worker processes.  It will cause them to
    immediately enter a blocking receive for TAG_CONFIG.  The supervisor process
    will send each worker the configuration section for the components it will
    be hosting.  Every worker will get the Global section; other sections will
    each be sent to just one woker each.
    """

    world = MPI.COMM_WORLD
    assignments = world.recv(source=SUPERVISOR_RANK, tag=TAG_CONFIG)
    return assignments


def distribute_assignments_supervisor(argvals):
    """Parse config file and distribute component assignments

    :param argvals: Arguments structure parsed by argparse.  
    :return: Dictionary of component definitions for components assigned to this
                    process (i.e., the supervisor).

    This function should be called only by the supervisor process.  It will
    parse the configuration file and distribute the instantiated components to
    the workers.  Apart from the global parameters component, which is
    distributed to all workers, components are distributed round-robin.

    Each component may have an optional parameter called `mp.weight`, which will
    affect the way the components are assigned.  Currently the weight just
    affects the order of the round-robin assignment; components are assigned in
    descending order of weight, minimizing the likelihood that two heavy
    components will end up on the same node.  (This only matters if there are
    fewer nodes than components.)  Future enhancements may attempt to balance
    (approximately) the total weight on each node, so that a large collection of
    light components could all be assigned to a single node, rather than being
    shared out amongst the nodes hosting heavy components.
    """

    from configobj import ConfigObj

    config = ConfigObj(argvals.ctlfile)

    # Get list of section names 
    section_names = list(config.keys())
    try:
        # Global section goes to everyone.  Also, it's required, so check for it
        # here
        section_names.remove('Global')
    except ValueError as e:
        raise RuntimeError("Config file must have a '[Global]' section") from e
    
    section_weights = [config[s].get('mp.weight', 1.0) for s in section_names]
    name_weight = zip(section_names, section_weights)
    section_names = [s[0] for s in sorted(name_weight, key=lambda x:x[1], reverse=True)]

    world = MPI.COMM_WORLD
    nproc = world.Get_size()
    nextrank = (SUPERVISOR_RANK+1) % nproc

    assignments = []
    for i in range(nproc):
        assignments.append({'Global':config['Global']})
    for section in section_names:
        assignments[nextrank][section] = config[section]
        nextrank = (nextrank+1)%nproc

    # Distribute these assignments to the workers
    for r in range(nproc):
        if r != SUPERVISOR_RANK:
            logging.debug(f'sending assignment to rank {r}: {assignments[r]}')
            world.send(assignments[r], dest=r, tag=TAG_CONFIG)

    logging.debug(f'supervisor assignment: {assignments[SUPERVISOR_RANK]}')
    return assignments[SUPERVISOR_RANK]

    
def finalize(rab, thread):
    """Finalization procedure for mp calculations.

    :param rab: The RAB object for this worker or supervisor process.
    :param thread: Thread object for the thread running the RAB.

    The first thing we do here is execute a barrier.  This ensures that no
    component will proceed with the shutdown procedure until all processes have
    finished their work.  In the meantime, all RABs will remain active servicing
    requests from components that are still running.

    Once all components have finished, all processes need to shut down their
    RABs and exit.
    """

    logging.debug(f'{rab.comm.Get_rank()} entering finalize.')
    
    rab.comm.barrier()

    rab.shutdown()
    thread.join()

# End of finalize()

