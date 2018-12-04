"""Definition of the RAB class.

The RAB provides access to capabilities hosted on remote nodes in MP
calculations.  It implements much of the component interface, but it isn't a
component as such, and in some respects it works very differently to proper
components.

The name RAB was originally an acronym for "Remote Access Broker", but mostly
it's just a name.  Also, "RAB" are my mom's initials.  Hi, Mom!

This module should _only_ be imported by the my.py module.  It imports mpi4py,
which will have the side effect of trying to initialize MPI if mp.py hasn't
already done it.
"""

from cassandra.constants import TAG_REQ, TAG_REQID_BASE
from mpi4py import MPI
import concurrent.futures as ft
import threading
import logging

# The choice of the sleep length in RAB listener loop is a tradeoff.  Making it
# shorter improves responsiveness, but creates more waiting in the GIL,
# potentially slowing down threads that are actually running models (especially
# if those models are implemented in python and running in the same process).
RAB_LOOP_SLEEP = 0.05          # 50 ms


class RAB(object):
    def __init__(self, cap_tbl, comm=MPI.COMM_WORLD):
        self.cap_tbl = cap_tbl
        self.comm = comm
        self.rank = comm.Get_rank()
        self.status = 0         # status indicator follows ComponentBase convention
        self.terminate = False  # sentinel indicating when it's time for the RAB to exit
        self.remote_caps = {}   # Table of remote capabilities
        self.requests_outstanding = {}  # Table of requests in process

        # members for managing message tags
        self.taglock = threading.Condition()  # Lock for working with the list of tags
        self.tags = set()                    # MPI message tags in use

    def run(self):
        """Execute the RAB's listen() method in a separate thread."""
        thread = threading.Thread(target=lambda: self.listen_wrap())
        thread.start()
        # returns immediately
        return thread

    def shutdown(self):
        """Order the RAB to exit its listen() thread."""
        # The GIL should take care of any locking that needs to be done here.
        logging.debug(f'{self.comm.Get_rank()}: shutdown.')
        self.terminate = True

    def addremote(self, remote_rank, remote_cap_tbl):
        """Add capabilities from a remote process to our remote capability table.

        :param remote_rank: MPI rank of the remote process
        :param remote_cap_tbl: List of capabilities held by the remote process 
        """

        for cap in remote_cap_tbl:
            if cap in self.cap_tbl:
                raise RuntimeError(f'Duplicate definition of capability {cap}.')
            self.cap_tbl[cap] = self
            self.remote_caps[cap] = remote_rank

    def unique_tag(self):
        """Get a message tag not already in use by another thread.
        """
        # Start with the base value of TAG_REQ, and search forward until we find
        # one that isn't already in use.  We have to acquire a lock to do this,
        # in order to avoid a race condition if another thread is trying to get
        # a tag assignment at the same time.
        tag = TAG_REQ
        with self.taglock:
            while tag in self.tags:
                tag += 1
            self.tags.add(tag)
            return tag

    def fetch(self, capability):
        """Fetch a capability from a remote process.

        This method uses blocking sends and receives, so it will automatically
        block the calling thread until the data is available (i.e., without
        having to wait on a condition variable).

        We use MPI message tags to disambiguate multiple requests from different
        threads on the same node.  By issuing a unique tag for each request, we
        ensure that the combination of source and tag uniquely identifies a
        particular request in the system.  Thus, if the responding RAB sends its
        result back to the source of the request, using the same tag as the one
        sent with the request, then we can be certain that the result will be
        delivered to the thread that requested it.

        """

        # This call could have come either from a component asking for a remote
        # capability, or from a spawned thread from the listener.  In the latter
        # case, we need to forward the request to the component that has the
        # data.  We *don't* raise CapabilityNotFound exceptions here because
        # everything that gets here should have been forwarded by a component's
        # fetch method, which will already have checked for the capability's
        # existence.
        provider = self.cap_tbl[capability]
        if self is not provider:
            return provider.fetch(capability)

        provider_rank = self.remote_caps[capability]
        reqtag = self.unique_tag()  # get a unique tag for the response

        # send both the capability and the tag we will be expecting for the
        # response to the remote RAB
        data = (capability, reqtag)
        logging.debug(f'requesting {capability} from {provider_rank} on tag {reqtag}')
        self.comm.send(data, dest=provider_rank, tag=TAG_REQ)
        # wait for the response
        logging.debug(f'waiting on {provider_rank} with tag {reqtag}')
        rslt = self.comm.recv(source=provider_rank, tag=reqtag)
        logging.debug(f'got {reqtag} from {provider_rank}')
        return rslt

    def listen_wrap(self):
        """Thread wrapper for the listen() method.

        The purpose of this wrapper is to run listen() and ensure that in the
        event of a failure MPI_Abort gets called, so as not to hang the system.

        """

        try:
            return self.listen()
        except:
            self.status = 2
            logging.exception('Exception in RAB.listen().  Calling MPI_Abort.')
            MPI.COMM_World.Abort()
            raise

    def listen(self):
        """Main control loop for RAB listener.

        The control loop proceeds as follows:

        1. Check for incoming requests from other nodes.
        2. If a request is outstanding:
           A. Spawn a thread to fetch the data required (using a concurrent.future)
           B. Record the source of the request alongside the Future object in the
              table of running requests
           C. repeat until no further requests are outstanding
        3. Check running requests to see if any have completed.  If so:
           A. Harvest the result.
           B. Look up the original source of the request
           C. Send (nonblocking) the result to the requestor
           D. Remove the completed Future from the request table
           E. Repeat until there are no further running requests.
        4. Check whether we have been asked to shut down.  If so, terminate.
        5. Sleep for delay = RAB_LOOP_SLEEP
        6. Repeat until we terminate in step 4.
        """

        from time import sleep
        with ft.ThreadPoolExecutor() as self.executor:
            logging.debug(f'begin listen')
            while True:
                ### Check for incoming requests.
                self.process_incoming()

                ### Check outstanding requests (the ones we have running)
                self.process_outstanding()

                ### Check for termination request.  The barrier in mp.finalize()
                ### ensures that this can't happen until all other components on
                ### this and other nodes have finished; therefore, all the
                ### outstanding requests will have been processed.
                if self.terminate:
                    logging.debug('listen loop got request to terminate')
                    assert(len(self.requests_outstanding) == 0)
                    break

                sleep(RAB_LOOP_SLEEP)

        logging.debug(f'{self.comm.Get_rank()}: listen exiting')
        return 0

    def process_incoming(self):
        """Dispatch incoming requests from other nodes, if any.
        """

        stat = MPI.Status()
        while self.comm.iprobe(tag=TAG_REQ, status=stat):
            source = stat.Get_source()
            capability, rtag = self.comm.recv(source=source)
            logging.debug(f'{self.rank}: processing {capability} from {source} on tag {rtag}')

            # Create a thread to fetch the capability.  This thread might block.
            future = self.executor.submit(self.fetch, capability)

            # Add the source and the remote tag to the table, indexed by thread.
            # We don't need the capability anymore, so we don't store it.
            self.requests_outstanding[future] = (source, rtag)

    # End of process_incoming()

    def process_outstanding(self):
        """Process any threads that have finished servicing their requests.

        For each thread in the list of outstanding requests, check to see if it
        has completed.  If so, send the response back to the requestor and
        remove the thread object from the outstanding requests list.

        """

        threads = list(self.requests_outstanding.keys())
        for thread in threads:
            if not thread.done():
                continue

            rslt = thread.result() # no need to specify a timeout, since we
                                   # already verified that the thread completed.

            source, rtag = self.requests_outstanding.pop(thread)
            logging.debug(f'sending result to {source} on tag {rtag}')
            # theoretically this could block, but the fetch method on the remote
            # node will have posted a receive as soon as the request was sent.
            self.comm.send(rslt, dest=source, tag=rtag)
            logging.debug(f'sent {rtag} to {source}')

    # End of process_outstanding
