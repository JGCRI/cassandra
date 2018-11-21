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

from mp.py import TAG_REQ, TAG_DATA
from mpi4py import MPI

RAB_LOOP_SLEEP = 0.025          # 25 ms

class RAB(object):
    def __init__(self, cap_tbl, comm=MPI.COMM_WORLD):
        self.cap_tbl = cap_tbl
        self.comm = comm
        self.status = 0         # status indicator follows ComponentBase convention
        self.terminate = False   # sentinel indicating when it's time for the RAB to exit
        self.remote_caps = {}   # Table of remote capabilities
        self.requests_outstanding {} # Table of requests in process

    def run(self):
        """Execute the RAB's listen() method in a separate thread."""
        thread = threading.Thread(target=lambda: self.listen())
        thread.start()
        # returns immediately
        return thread

    def shutdown(self):
        """Order the RAB to exit its listen() thread."""
        # The GIL should take care of any locking that needs to be done here.
        self.terminate = True

    def addremote(self, remote_rank, remote_cap_tbl):
        """Add capabilities from a remote process to our remote capability table.

        :param remote_rank: MPI rank of the remote process
        :param remote_cap_tbl: List of capabilities held by the remote process 
        """

        for cap in remote_cap_tbl:
            if capability in self.cap_tbl:
                raise RuntimeError(f'Duplicate definition of capability {cap}.')
            self.cap_tbl[cap] = self
            self.remote_caps[cap] = remote_rank
        
        
    def fetch(self, capability):
        """Fetch a capability from a remote process.

        This method uses blocking sends and receives, so it will automatically
        block the calling thread until the data is available (i.e., without
        having to wait on a condition variable).

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
        self.comm.send(capability, dest=provider_rank, tag=TAG_REQ)
        # wait for the response
        return self.comm.recv(source=provider_rank, tag=TAG_DATA)

    
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
        while True:
            ### Check for incoming requests.
            self.process_incoming()

            ### Check outstanding requests (the ones we have running)
            self.process_outstanding()

            ### Check for termination request - note that this can't happen
            ### until all other components on this and other nodes have
            ### finished; therefore, all the outstanding requests will have been
            ### processed.
            if self.terminate:
                assert len(self.requests_outstanding) == 0
                break

            sleep(RAB_LOOP_SLEEP)

        # Record our status as successful
        self.status = 1
        return 0
    
