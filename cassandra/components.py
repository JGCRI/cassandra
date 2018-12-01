"""Definitions for the components for models participating in the framework.

A "component" is a functional unit that performs a specific step in
the coupled model processing pipeline.  Generally, a component will
correspond to one of the models being coupled, but components can also
implement utility processes, such as formatting and packaging output,
and particularly complex models could be split over several components.

Classes:

CapabilityNotFound    - Exception class raised when a component requests
                        a capability that is not provided by any
                        component in the system.

ComponentBase         - Base class for all components.  Provides the
                        interface, as well as services like managing
                        threads, locks, and condition variables.

GlobalParamsComponent - Store parameters common to all components.

GcamComponent         - Run the GCAM core model.

TethysComponent       - Run the Tethys spatiotemporal global water use
                        downscaling model.

XanthosComponent      - Run the Xanthos global hydrology model.

HectorStubComponent   - Serve Hector output for RCP scenarios.

DummyComponent        - A simple component class for tests.

"""

# TODO: many of these classes have gotten a bit long.  It would be
# better to refactor them so that the main functionality is
# implemented in a separate python module for each component, with
# the class derived from ComponentBase providing a thin wrapper that grabs inputs
# needed from other components and passes them to a main function in the
# relevant python component.

import os
import os.path
import re
import subprocess
import threading
import tempfile
import pkg_resources
import pandas as pd
from sys import stdout
from sys import stderr
from cassandra import util

# This class is here to make it easy for a class to ignore failures to
# find a particular capability in fetch() while still failing on any
# other sort of error.


class CapabilityNotFound(RuntimeError):
    pass


class ComponentBase(object):
    """Common base class for all components (i.e., functional units) in the system.

    We can put any utility functions that are common to all components
    here, but its main purpose is to provide all the multithreading
    functionality so that the individual components can focus
    exclusively on doing their particular tasks.

    Methods that shouldn't be overridden:

    run(): start the component running.  The params argument should
           be a dictionary containing the parameters the component
           needs (probably parsed from the initial config).  Each
           subclass should provide a method called run_component()
           that performs the component's work; that method will be
           called from run().  The run_component() method should
           return 0 on successful completion.  Note that we don't
           make any effort to limit the number of concurrent threads
           beyond the inherent limitations imposed by data
           dependencies between the components.  This method returns
           the thread object, mainly so that the driver can call
           join() on all of the component threads.
           TODO: implement an active thread counter.

    run_component_wrapper(): used internally by run().  Don't monkey around
                             with this function.

    fetch(): retrieve the component's results for a single capability.
             Takes a capability name as an argument.  If the capability
             does not belong to this component, performs a lookup and
             calls fetch() on the component that has the requested data.
             If the component hasn't completed yet, wait to be notified
             of completion.  This mechanism implicitly enforces correct
             ordering between components.  Note that we don't make any
             checks for deadlock caused by circular dependencies.

    addparam(): Add a key and value to the params array.  Generally
                this should only be done in the config file parser.

    addcapability(): Add a capability to the capability table.

    addresults(): Update the results for a single capability. Use this
                  rather than updating self.results directly, as this
                  method ensures that the capability exists.

    Methods that can be extended (but not overridden; you must be sure
         to call the base method):

    __init__(): initialization, obviously, but each init method must
                take an extra argument that is a dictionary of component
                capabilities.
                The base class init stores a reference to the
                capability table for future lookup.  A component may
                optionally call addcapability() here to add
                capabilities that are independent of any parameters.

    finalize_parsing(): When parsing is complete, what you have is a
                bunch of key-value pairs.  This function is the place
                to do any processing that needs to be done (e.g.,
                converting strings to other types).  The base class
                version of the method does this for parameters that
                are applicable to all components, so it must always be
                called if the method is overridden.  A component may
                also call addcapability() from this method to add
                capabilities that depend on the component's
                parameters.

    Methods that can be overridden freely

    run_component(): function that does the component's work.  It should only be
              called from the run_component_wrapper() method via the run()
              method.  Together, these methods perform the additional
              bookkeeping required to ensure that components don't try to
              use results before they are ready.

    Attributes:

    params: dictionary of parameters parsed from the config file.
            Generally this array should be altered only by calling the
            addparam method.

    """

    def __init__(self, cap_tbl):
        """Initialize the component base.

        Every subclass __init__ method should call this method as its
        first action.  The cap_tbl argument is a dictionary linking
        capabilities (i.e., tags for identifying functional units)
        with the components that provide those capabilities.  Subclasses
        should extend this method by adding their self reference to
        the table under an appropriate tag.  E.g.:

        cap_tbl['gcam_core'] = self

        The capability table will be available as self.cap_tbl for use
        in a subclass's methods.  Since cassandra passes the same
        capacity table to each subclass instance initialization, by
        the time a component starts running the table will contain an
        index of all the active components in the calculation.

        """
        self.status = 0         # status indicator: 0- not yet run, 1- complete, 2- error
        self.results = {}
        self.params = {}
        self.cap_tbl = cap_tbl  # store a reference to the capability lookup table
        self.condition = threading.Condition()

    def run(self):
        """Execute the component's run_component() method in a separate thread."""
        thread = threading.Thread(target=lambda: self.run_component_wrapper())
        thread.start()
        # returns immediately
        return thread

    def run_component_wrapper(self):
        """Lock the condition variable, execute run_component(), and unlock when it returns.

        At the conclusion of the run_component() method, self.status will be
        set to 1 if the run was successful, to 2 otherwise.  This
        variable will be used by the fetch() method to notify clients
        if a run failed.  Either way, threads waiting on the condition
        variable will be released when the run completes.

        At the end of this function the following will be true:

        1. Either self.status==1 or self.status==2

        2. If self.status==1, the self.results has a full set of
        results in it.

        This function should be called *only* by the run() method
        above.

        """

        # This block holds the lock on the condition variable for the
        # entire time the run_component() method is running.  That's ok for
        # now, but it's not ideal, and it will cause problems when we
        # eventually try to implement co-simulations.
        with self.condition:
            try:
                rv = self.run_component()
                if not rv == 0:
                    # possibly add some other error handling here.
                    raise RuntimeError(f"{self.__class__}:  run_component returned error code {str(rv)}")
                else:
                    stdout.write(f"{self.__class__}: finished successfully.\n")

                self.status = 1                  # set success condition
            except:
                self.status = 2                  # set error condition
                raise
            finally:
                self.condition.notify_all()      # release any waiting threads
        # end of with block:  lock on condition var released.

    def fetch(self, capability):
        """Return the data associated with the named capability.

        Components don't return results from run() because it will run
        asynchronously.  Instead, if you want the results associated
        with a particular capability, you call this method with the
        name of the capability.  If the capability does not exist in
        the system, a CapabilityNotFound exception will be thrown.

        Internally, this method first looks up the component that has
        the requested data and forwards the request to that
        component's fetch method.  That call waits if necessary, then
        checks whether the run was successful (indicated by
        self.status), and if so returns the requested data.  If the
        run_component() method failed, the variable will so indicate,
        and an exception will be raised.

        Components should store their results by calling
        self.addresults(capability-name, data), which adds the results
        in the self.results dictionary, which is where this method
        will look for them.  The system levies no particular
        requirements on the format of data returned, so components
        should publish a complete description of their data in their
        documentation.

        WARNING: if a component tries to fetch a capability that it,
        itself, provides, this will lead to instant deadlock.  So,
        don't do that.

        """

        try:
            provider = self.cap_tbl[capability]
        except KeyError:
            raise CapabilityNotFound(capability)

        if self is not provider:
            # This is a request (presumably originating in our own run
            # method) for a capability in another component.  Forward
            # it to that component.
            return provider.fetch(capability)

        # If we get to here, then this is a request from another
        # component for some data we are holding.

        # If the component is currently running, then the condition
        # variable will be locked, and we will block when the 'with'
        # statement tries to obtain the lock.
        with self.condition:
            if self.status == 0:                  # component hasn't run yet.  Wait on it
                print(f"\twaiting on {self.__class__}\n")
                self.condition.wait()
        # end of with block:  lock is released

        # By this point, the component should have run.  If status is not success, then
        # there has been an error.
        if self.status != 1:
            raise RuntimeError(f"{self.__class__}: wait() returned with non-success status!")

        return self.results[capability]

    def finalize_parsing(self):
        """Process parameters that are common to all components (e.g. clobber).

        The components will be responsible for processing their own
        special parameters.  If a component needs to override this
        method, it should be sure to call the base version too.

        """

        self.clobber = True          # default to overwriting outputs
        if "clobber" in self.params:
            self.clobber = util.parseTFstring(self.params["clobber"])

        # processing for additional common parameters go here
        return

    def addparam(self, key, value):
        """Add a parameter key and value parsed from the config file.

        In the current design, this should be called only by the
        config file parser.

        """

        self.params[key] = value

    def addcapability(self, capability):
        """Add a capability to the capability table."""
        if capability in self.cap_tbl:
            raise RuntimeError(f'Duplicate definition of capability {capability}.')
        self.cap_tbl[capability] = self

    def addresults(self, capability, res):
        """Add data to the specified capability of this component."""
        if capability not in self.cap_tbl:
            raise CapabilityNotFound(capability)

        if self.cap_tbl[capability] is not self:
            raise RuntimeError(f'Component {self.__class__} does not own capability {capability}.')

        self.results[capability] = res

    def run_component(self):
        """Subclasses of ComponentBase are required to override this method.

        Components' implementations of this method should add the
        results of their calculations to the self.results dictionary
        by calling self.addresults(<capability-name>, data), where
        capability-name is the name of the capability being provided.
        A component can provide multiple capabilities, with each one
        getting its own entry in the results dictionary.
        """

        raise NotImplementedError("ComponentBase is not a runnable class.")


# class to hold the general parameters.
class GlobalParamsComponent(ComponentBase):
    """Class to hold the general parameters for the calculation.

    Technically this isn't a component as such; it doesn't run anything,
    but treating it as a component allows us to parse it using the same
    code we use for all the real components, and having it in the
    capability table makes it easy for any component that needs one of
    the global parameters to look them up.

    Parameters:

    ModelInterface - Location of the jar file for the ModelInterface
                     code, used to query GCAM outputs.

       DBXMLlib - Location of the DBXML libraries used by older
                  versions of the ModelInterface code.

       inputdir - Directory containing general input files.  (OPTIONAL
                  - default is './input-data').  Relative paths will
                  be interpreted relative to the working directory
                  (even if they don't begin with './')

      rgnconfig - Directory containing region configuration files.
                  Any data that changes with the the region mapping
                  should be in this directory.  The directory will be
                  converted to an absolute path if it does not start
                  with '/'.  If it starts with './' the path will be
                  relative to the directory the driver code is running
                  in; otherwise, it will be relative to inputdir.
                  (OPTIONAL - default is 'rgn14')

    """

    def __init__(self, cap_tbl):
        """Copy parameters into results dictionary.

        This function also sets the genparams attribute of the util
        component, since it can't get them from this class directly.

        """
        super(GlobalParamsComponent, self).__init__(cap_tbl)

        self.addcapability('general')

        # this is a reference copy, so any entries added to params will also appear in results.
        self.addresults('general', self.params)

        print('General parameters as input:')
        print(self.results['general'])

        # We need to allow gcamutil access to these parameters, since it doesn't otherwise know how to find the
        # global params component.  <- gross.  we need a better way to do this.
        util.global_params = self

    def run_component(self):
        """Set the default value for the optional parameters, and convert filenames to absolute paths."""
        genrslt = self.results['general']
        genrslt['ModelInterface'] = util.abspath(self.results['general']['ModelInterface'])
        genrslt['DBXMLlib'] = util.abspath(self.results['general']['DBXMLlib'])

        if 'inputdir' in genrslt:
            inputdir = genrslt['inputdir']
        else:
            inputdir = './input-data'
        genrslt['inputdir'] = util.abspath(inputdir, os.getcwd())

        if 'rgnconfig' in genrslt:
            rgnconfig = genrslt['rgnconfig']
        else:
            stdout.write('[GlobalParamsComponent]: Using default region mapping (14 region)')
            rgnconfig = 'rgn14'
        genrslt['rgnconfig'] = util.abspath(rgnconfig, genrslt['inputdir'])

        return 0                # nothing to do here.


class GcamComponent(ComponentBase):
    """Provide the 'gcam-core' capability.

    This component runs the GCAM core model, making the location of the
    output database available under the 'gcam-core' capability.

    Parameters:
      exe        = full path to gcam.exe
      config     = full path to gcam configuration file
      logconfig  = full path to gcam log configuration file
      clobber    = flag: True = clobber old outputs, False = preserve old outputs

    Results:
      dbxml      = gcam dbxml output file.  We get this from the gcam config.xml file.

    Component dependencies: none

    """

    def __init__(self, cap_tbl):
        """Add self to the capability table."""
        super(GcamComponent, self).__init__(cap_tbl)
        self.addcapability('gcam-core')

    def run_component(self):
        """Run the GCAM core model.

        We start by checking to see that all the input files needed
        for GCAM to run are actually available.  If any of them are
        missing, we raise an IOError execption.  Next we parse the
        config.xml file to find out what outputs we expect, and we
        check to see if they are already present.  If they are, and if
        'clobber' is not set to True, then we skip the run and return
        the location of the existing dbxml.  Otherwise, we do the GCAM
        run and then return the dbxml location.

        """

        # Process the parameters
        exe = self.params["exe"]
        cfg = self.params["config"]
        logcfg = self.params["logconfig"]
        try:
            logfile = self.params['logfile']  # file for redirecting gcam's copious stdout
        except KeyError:
            # logfile is optional
            logfile = None

        # usually the exe, cfg, and logcfg files will be in the same
        # directory, but in case of difference, take the location of
        # the config file as controlling.
        self.workdir = os.path.dirname(exe)

        msgpfx = "GcamComponent: "    # prefix for messages coming out of this component
        # Do some basic checks:  do these files exist, etc.
        if not os.path.exists(exe):
            raise IOError(msgpfx + "File " + exe + " does not exist!")
        if not os.path.exists(cfg):
            raise IOError(msgpfx + "File " + cfg + " does not exist!")
        if not os.path.exists(logcfg):
            raise IOError(msgpfx + "File " + logcfg + " does not exist!")

        # we also need to get the location of the dbxml output file.
        # It's in the gcam.config file (we don't repeat it in the
        # config for this component because then we would have no way to
        # ensure consistency).
        dbxmlfpat = re.compile(r'<Value name="xmldb-location">(.*)</Value>')
        dbenabledpat = re.compile(r'<Value name="write-xml-db">(.*)</Value>')

        # get a reference to the results that we will be exporting
        gcamrslt = {}

        with open(cfg, "r") as cfgfile:
            # we don't need to parse the whole config file; all we
            # want is to locate the name of the output file make sure
            # the dbxml output is turned on.
            dbxmlfile = None
            for line in cfgfile:
                # the dbxml file name will come early in the file
                match = dbxmlfpat.match(line.lstrip())
                if match:
                    dbxmlfile = match.group(1)
                    break

            print(f"{self.__class__}:  dbxmlfile = {dbxmlfile}")
            # The file spec is a relative path, starting from the
            # directory that contains the config file.
            dbxmlfile = os.path.join(self.workdir, dbxmlfile)
            gcamrslt["dbxml"] = dbxmlfile  # This is our eventual output
            if os.path.exists(dbxmlfile):
                if not self.clobber:
                    # This is not an error; it just means we can leave
                    # the existing output in place and return it.
                    print("GcamComponent:  results exist and no clobber.  Skipping.")
                    gcamrslt["changed"] = 0  # mark the cached results as clean
                    return 0
                else:
                    # have to remove the dbxml, or we will merely append to it
                    os.unlink(dbxmlfile)

            # now make sure that the dbxml output is turned on
            for line in cfgfile:
                match = dbenabledpat.match(line.lstrip())
                if match:
                    if match.group(1) != "1":
                        raise RuntimeError(
                            msgpfx + "Config file has dbxml input turned off.  Running GCAM would be futile.")
                    else:
                        break

        # Add our output structure to the results dictionary.
        self.addresults('gcam-core', gcamrslt)

        # now we're ready to actually do the run.  We don't check the return code; we let the run() method do that.
        print(f"Running:  {exe} -C{cfg} -L{logcfg}")

        if logfile is None:
            return subprocess.call([exe, '-C'+cfg, '-L'+logcfg], cwd=self.workdir)
        else:
            with open(logfile, "w") as lf:
                return subprocess.call([exe, '-C'+cfg, '-L'+logcfg], stdout=lf, cwd=self.workdir)


class TethysComponent(ComponentBase):
    """Class for the global water withdrawal downscaling model Tethys.

    This component makes use of the Tethys package, an open-source
    spatiotemporal water demand downscaling model.

    The results are global annual gridded water withdrawal by sector,
    providing a capability for each Tethys output sector. Units are
    specified by the Tethys configuration file. If Tethys is set up to
    run with temporal downscaling, additional capabilities for monthly
    results will be available.

    For more information: https://github.com/JGCRI/tethys

    params:
       config_file - path to Tethys config file
    """

    def __init__(self, cap_tbl):
        super(TethysComponent, self).__init__(cap_tbl)

        # Map the capability name to the corresponding Tethys result
        self.capability_map = {
            "gridded_water_demand_dom": "wddom",      # Domestic
            "gridded_water_demand_elec": "wdelec",    # Electricity Generation
            "gridded_water_demand_irr": "wdirr",      # Irrigation
            "gridded_water_demand_liv": "wdliv",      # Livestock
            "gridded_water_demand_mfg": "wdmfg",      # Manufacturing
            "gridded_water_demand_min": "wdmin",      # Mining
            "gridded_water_demand_nonag": "wdnonag",  # Non-Agricultural
            "gridded_water_demand_total": "wdtotal"   # Total
        }
        self.temporal_sectors = {
            "gridded_monthly_water_demand_dom": "twddom",    # Domestic
            "gridded_monthly_water_demand_elec": "twdelec",  # Electricity Generation
            "gridded_monthly_water_demand_irr": "twdirr",    # Irrigation
            "gridded_monthly_water_demand_liv": "twdliv",    # Livestock
            "gridded_monthly_water_demand_mfg": "twdmfg",    # Manufacturing
            "gridded_monthly_water_demand_min": "twdmin",    # Mining
        }

        for cap in self.capability_map.keys():
            self.addcapability(cap)

    def finalize_parsing(self):
        super(TethysComponent, self).finalize_parsing()

        # Check if Tethys is running with temporal downscaling (an optional output)
        from configobj import ConfigObj

        tethys_config = ConfigObj(self.params['config_file'])
        temporal_downscaling = tethys_config['Project']['PerformTemporal']

        # If it is, add the temporal downscaling capabilities
        if temporal_downscaling:
            for cap in self.temporal_sectors.keys():
                self.addcapability(cap)
            self.capability_map.update(self.temporal_sectors)

    def run_component(self):
        """Run Tethys."""
        from tethys.model import Tethys

        config_file = self.params["config_file"]

        # run the Tethys model
        tethys_results = Tethys(config=config_file)

        for capability_name, tethys_attr in self.capability_map.items():
            self.addresults(capability_name, getattr(tethys_results.gridded_data, tethys_attr))

        return 0


class XanthosComponent(ComponentBase):
    """Class for the global hydrologic model Xanthos

    This component makes use of the Xanthos package, an open-source
    hydrologic model.

    For more information: https://github.com/JGCRI/xanthos

    params:
       config_file - path to Xanthos config file
    """

    def __init__(self, cap_tbl):
        super(XanthosComponent, self).__init__(cap_tbl)
        self.addcapability("gridded_runoff")

    def finalize_parsing(self):
        """Load the reference file mapping Xanthos cell index to lat/lon."""
        super(XanthosComponent, self).finalize_parsing()

        from configobj import ConfigObj
        import pandas as pd

        xanthos_config = ConfigObj(self.params['config_file'])
        root_dir = xanthos_config['Project']['RootDir']
        in_dir = xanthos_config['Project']['InputFolder']
        ref_dir = xanthos_config['Project']['RefDir']

        cell_map_path = os.path.join(root_dir, in_dir, ref_dir, 'coordinates.csv')
        xcolnames = ['cell_id', 'lon', 'lat', 'lon_idx', 'lat_idx']

        self.cell_map = pd.read_csv(cell_map_path, names=xcolnames)

    def run_component(self):
        """Run Xanthos."""
        import xanthos

        config_file = self.params["config_file"]

        xth = xanthos.Xanthos(config_file)

        args = {}

        # Wait for other components to produce precipitation and
        # temperature data (as pandas DataFrames), if provided
        self.add_args_from_capability(args, 'PrecipitationFile', 'gridded_pr', 'mm_month-1')
        self.add_args_from_capability(args, 'trn_tas', 'gridded_tas', 'C')

        xth_results = xth.execute(args)

        # Add runoff (Q) results from xanthos to the gridded_runoff capability
        self.addresults("gridded_runoff", xth_results.Q)

        return 0

    def add_args_from_capability(self, args, arg_name, cap_name, units=None):
        """Get Xanthos parameters from another capability.

        If the no component provides the requested capability, no arguments are added.

        params:
          args: Reference to dictionary passed to Xanthos
          arg_name: Name of argument to add
          cap_name: Name of capability with required value
          units: Optional string to use to assert correct units
        """
        if cap_name in self.cap_tbl:
            arg_val = self.fetch(cap_name)
            assert units is None or arg_val['units'] == units
            args[arg_name] = self.prep_for_xanthos(arg_val)

    def prep_for_xanthos(self, monthly_data):
        """Convert a fldgen output array to Xanthos' expected input format.

        Retrieve Xanthos grid cells from alternately indexed vectors.

        Convert spatial data from a list of numeric vectors (each list element is one
        month) to the input format expected by Xanthos (land cell x months).

        params:
          monthly_data: Input data for xanthos as numpy array (cells x months)

        returns:
          2d array of Xanthos cells by month

        """
        ycolnames = c('index', 'lat', 'lon')
        ycells = pd.DataFrame(monthly_data['data'].T, columns=ycolnames)

        bothcells = ycells.merge(self.cell_map, on=['lat', 'lon'])

        # Order by Xanthos cell id for indexing
        bothcells.sort_values(by='cell_id', inplace=True)

        # Re-order the data to correct order by extracting with the cell index
        cells = monthly_data[:, bothcells.index.astype(int)]

        # Rows and columns need to be flipped
        return cells.T


class FldgenComponent(ComponentBase):
    """Class for the precipitation and temperature grids

    This component makes use of the fldgen code. That code lives in its
    own repository and must be installed independently.

    To start, only works with the streamlined branch modified not to
    call Xanthos.

    params:
       workdir  - working directory (location of frontEnd_grandExp code)

    results: pr and temp grids
    """

    def __init__(self, cap_tbl):
        super(FldgenComponent, self).__init__(cap_tbl)
        self.addcapability("gridded_pr")
        self.addcapability("gridded_tas")

    def run_component(self):
        """Run the fldgen R scripts."""
        from rpy2.robjects.packages import importr
        import rpy2.robjects as robjects
        import numpy as np

        workdir = self.params["workdir"]

        an2month = os.path.join(workdir, "an2month")
        fldgen = os.path.join(workdir, "fldgen")
        gexp_funs = os.path.join(workdir, "frontEnd_grandExp/grand_experiment_functions.R")
        emulator_mapping = os.path.join(workdir, "input", "emulator_mapping.csv")
        hector_runs = os.path.join(workdir, "input", "hector_tgav-rcp45.csv")

        devtools = importr("devtools")
        devtools.load_all(an2month)
        devtools.load_all(fldgen)

        rsource = robjects.r["source"]
        rsource(gexp_funs)

        grand_experiment = robjects.r["grand_experiment"]
        r = grand_experiment(mapping=emulator_mapping, tgavSCN=hector_runs, xanthos_dir='', N=1)

        # r is a complex nested structure:
        # Emulator1 (e.g. ipsl-cm5a-lr)
        # -- Scenario1 (e.g. rcp45)
        # -- -- RunNum1 (e.g. 1)
        # -- -- -- Monthly Precipitation
        # -- -- -- -- Precip Data (months x 67420)
        # -- -- -- -- Precip Coordinates (3 (cell, lon, lat) x 67420)
        # -- -- -- -- Precip Units (e.g. mm_month-1)
        # -- -- -- Monthly Temperature
        # -- -- -- -- Temp Data (months x 67420)
        # -- -- -- -- Temp Coordinates (3 (cell, lon, lat) x 67420)
        # -- -- -- -- Temp Units (e.g. C)
        # -- -- RunNum2
        # -- -- -- ...
        # -- Scenario2
        # -- -- ...
        # Emulator2
        # -- ...
        # ...
        emulator_name = r[0]
        run_name = emulator_name[0]
        run_N = run_name[0]

        fldgen_pr = {
            'data': np.asarray(run_N[0][0]),
            'coords': np.asarray(run_N[0][1]),
            'units': run_N[0][2][0]  # All R strings are in vectors; take 1st element
        }

        fldgen_tas = {
            'data': np.asarray(run_N[1][0]),
            'coords': np.asarray(run_N[1][1]),
            'units': run_N[1][2][0]
        }

        self.addresults('gridded_pr', fldgen_pr)
        self.addresults('gridded_tas', fldgen_tas)

        return 0


class HectorStubComponent(ComponentBase):
    """Component to serve Hector output data for RCP scenarios.

    In cases where Hector output will be used only for the standard RCP
    scenarios, it is not necessary to run the model, as the outputs never
    change.  This module provides a way to read some of the most commonly used
    outputs for those scenarios.

    The component provides three capabilities:
      * Tgav   : global mean temperature
      * atm-co2: atmospheric CO2 concentration
      * Ftot   : total radiative forcing

    Each capability returns a data fram with data from all of the scenarios
    specified in the configuration.  Spinup time steps are not included.

    The parameters accepted by this component are:

    scenarios : comma separated list of scenarios to include
                e.g.: rcp26,rcp45
                If omitted, all four rcp scenarios are included.

    """

    def __init__(self, cap_tbl):
        super(HectorStubComponent, self).__init__(cap_tbl)
        self.addcapability('Tgav')
        self.addcapability('atm-co2')
        self.addcapability('Ftot')


    def run_component(self):
        """Run the HectorStub component
        
        Load the requested scenarios and make each variable available to the
        rest of the system.

        """

        import pandas as pd

        scenarios = [x.strip() for x in self.params['scenarios'].split(',')]
        scendata = pd.concat([self._read_scen_data(scen) for scen in scenarios])
        scendata['scenario'] = scendata['run_name']

        retcols = ['year', 'scenario', 'variable', 'value', 'units']
        
        self.addresults('Tgav', scendata[scendata['variable'] == 'Tgav'].loc[:, retcols])
        self.addresults('atm-co2', scendata[scendata['variable'] == 'Ca'].loc[:, retcols])
        self.addresults('Ftot', scendata[scendata['variable'] == 'Ftot'].loc[:, retcols])

        return 0

    def _read_scen_data(self, scen):
        """Read stored scenario data.

        :param scen: Scenario name to load.  One of rcp26, rcp45, rcp60, or 
                     rcp85.
        
        The spinup data will be filtered from the data that is read.

        """

        from os.path import join
        from pickle import load

        data = pkg_resources.resource_filename('cassandra', 'data')
        infile = open(join(data, f'hector-outputstream-{scen}.dat'),
                      'rb')
        df = load(infile)
        infile.close()

        return df[df['spinup'] == 0]

            
    
class DummyComponent(ComponentBase):
    """Dummy component for tests

    A dummy component with parameters for delaying requests and outputs in order
    to test interactions between multiple components.

    The idea is that if the the name of the capabilities being declared are
    derived from each component instance’s parameters, it is possible to
    configure a setup cleverly to create multiple copies that interact in any
    manner of one's choosing.

    params:
     capability_out - name of the output capability
    capability_reqs - list of the capabilities this component requests
     request_delays - list of time delays (ms) before each request is made
       finish_delay - delay (ms) before the component finalizes and exports
    """

    def __init__(self, cap_tbl):
        super(DummyComponent, self).__init__(cap_tbl)
        # most components add a capability here, but we can't do that
        # yet because we need our parameters before we can decide what
        # capability we are offering.

    def finalize_parsing(self):
        super(DummyComponent, self).finalize_parsing()

        # get this component's name and add it as a capability
        self.name = self.params['name']
        self.addcapability(self.name)

        # get this component's capability requirements
        if 'capability_reqs' in self.params:
            cr = self.params['capability_reqs']
            if not isinstance(cr, list):
                cr = [cr]
            self.capability_reqs = [s for s in cr if s != '']
        else:
            self.capability_reqs = []

        # get the request delays
        if 'request_delays' in self.params:
            rd = self.params['request_delays']
            if not isinstance(rd, list):
                rd = [rd]
            self.request_delays = [int(s) for s in rd if s != '']
        else:
            self.request_delays = []

        if len(self.capability_reqs) != len(self.request_delays):
            raise RuntimeError('Lengths of capability_reqs and request_delays must be the same.')

        # get the finish delay
        self.finish_delay = int(self.params['finish_delay'])

    def run_component(self):
        """Run, request, delay, output."""
        from time import time, sleep
        from logging import info
        from os import uname

        st = time()
        info(f'{st}: Start component on host {uname().nodename}')
        st_msg = (0, f'Start {self.name}')

        data = [st_msg]  # list of tuples: (time, message)

        capability_reqs = self.capability_reqs
        request_delays = self.request_delays
        finish_delay = self.finish_delay

        for i, req in enumerate(capability_reqs):
            delay = request_delays[i]
            sleep(delay / 1000.0)  # ms to s

            data.append((time() - st, f'Requesting data from {req}'))
            self.fetch(req)
            data.append((time() - st, f'Recieved data from {req}'))

        sleep(finish_delay / 1000.0)

        # Add our list of messages as the result for this capability
        self.addresults(self.name, data)

        data.append((time() - st, f'Done {self.name}'))

        return 0

    def report_test_results(self):
        """Report the component's results to the unit testing code."""
        return self.results[self.name]
