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

### Constants for MPI tags
TAG_CONFIG = 100                # configuration distribution method
TAG_CAPTBL = 101                # distribute capability tables
TAG_REQ = 102                   # Request data for a capability
TAG_DATA = 103                  # Response to TAG_REQ

TAG_DONE = 999                  # All components have finished


def mp_bootstrap(argvals):
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

    if rank==0:
        compsections = distribute_assignments_supervisor(argvals)
    else:
        compsections = distribute_assignments_worker(argvals)


    # compsections will be a list of dictionaries of configuration sections.
    # Each list element corresponds to an MPI rank.  We need to create and
    # initialize the components assigned to us.  We also need to create a RAB.
    cap_tbl = {}
    rab = RAB(cap_tbl)
    comps = [rab]
    for section, conf in compsections[rank].items:
        component = create_component(section, cap_tbl)
        component.params.update(conf)
        component.finalize_parsing()
        comps.append(component) 

    # Next we need to compile a table of remote capabilities.  To do this, each
    # component needs to distribute its local capability table to all the other
    # ranks, which we can do with an allgather collective.  Then each rank adds
    # all of the capabilities it got from other ranks to its RAB's list of
    # capabilities.
    allcaptbls = world.allgather(cap_tbl)

    for i, remote_cap in enumerate(allcaptbls):
        if i == rank:
            # skip our own contribution to the list
            continue

        rab.addremote(i, remote_cap)

    # Return the component list and capability table
    return (comps, cap_tbl)

