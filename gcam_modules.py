import os
import os.path
import re
import subprocess
#import threading
from gcamutil import *
## Definitions for the modules for GCAM and associated downstream models


#### A common base class for all of the modules.  We can put any
#### utility functions that are common to all modules here, but its
#### main purpose is to provide all the multithreading functionality
#### so that the individual modules can focus exclusively on doing
#### their particular tasks.
####
#### Methods that shouldn't be overridden:

####    run(): start the module running.  The params argument should
####           be a dictionary containing the parameters the module
####           needs (probably parsed from the initial config).  Each
####           subclass should provide a method called runmod() that
####           performs the module's work; that method will be called
####           from run().  The runmod() method should return 0 on
####           successful completion.  Note that we don't make any
####           effort to limit the number of concurrent threads beyond
####           the inherent limitations imposed by data dependencies
####           between the modules.  
####           TODO: implement an active thread counter.

####  fetch(): retrieve the module's results.  If the module hasn't
####           completed yet, wait to be notified of completion.  This
####           mechanism implicitly enforces correct ordering between
####           modules.  Note that we don't make any checks for
####           deadlock caused by circular dependencies.

#### addparam(): Add a key and value to the params array.  Generally
####             this should only be done in the config file parser.
####             And yes, I am aware that having a function to do this
####             is technically superfluous.

#### Methods that can be overridden, but you must be sure to call the
####    base method

#### __init__(): initialization, obviously, but each init method must
####             take an extra argument that is a dictionary of module
####             capabilities, and it must add its own capabilities to
####             this dictionary (see modules below for examples).
####             The base class init stores a reference to the
####             capability table for future lookup.

#### finalize_parsing(): When parsing is complete, what you have is a
####             bunch of key-value pairs.  This function is the place
####             to do any processing that needs to be done (e.g.,
####             converting strings to other types).  The base class
####             version of the method does this for parameters that
####             are applicable to all modules, so it must always be
####             called if the method is overridden.

#### Methods that can be overridden freely

#### runmod(): function that does the module's work.  It should only
####           be called from the run() method, which performs the
####           additional bookkeeping required to ensure that modules
####           don't try to use results before they are ready.  The
####           base class version raises a NotImplementedError.


#### Attributes:

####  params: dictionary of parameters parsed from the config file.
####          Generally this array should be altered only by calling
####          the addparam method.

class GcamModuleBase(object):
    def __init__(self, cap_tbl):
        self.complete = 0
        self.results = {}
        self.params  = {}
        self.results["changed"] = 1
        self.cap_tbl = cap_tbl # store a reference to the capability lookup table

    def run(self):
        ## TODO: have this run in a separate thread 
        self.runmod(params)
        self.complete = 1

    def fetch(self):
        ## get the results of the calculation.  These aren't returned
        ## from run() because it may run asynchronously.  This method
        ## waits if necessary and returns the results.
        if complete==0:
            ## once asynchronous components are implemented, we will
            ## wait() here, and this will no longer be an error.
            raise RuntimeError("Fetching data from a component that has not yet run.") 
        
        return self.results

    def finalize_parsing(self):
        ## process parameters that are common to all modules
        ## (e.g. clobber).  The modules will be responsible for
        ## processing their own special parameters.  If a module needs
        ## to override this method, it should be sure to call the base
        ## version too.
        self.clobber = True          # default to overwriting outputs
        if "clobber" in self.params: 
            self.clobber = parseTFstring(self.params["clobber"])

        ## processing for additional common parameters go here
        return

    def addparam(self, key, value):
        ## In the current design, this should be called only by the
        ## config file parser.
        self.params[key] = value

    def runmod(self):
        raise NotImplementedError("GcamModuleBase is not a runnable class.")

## class to hold the general parameters.  Technically this isn't a
## module as such; it doesn't run anything, but treating it as a
## module allows us to parse it using the same code we use for all the
## real modules, and having it in the capability table makes it easy
## for any module that needs one of the global parameters to look them
## up.
class GlobalParamsModule(GcamModuleBase):
    def __init__(self, cap_tbl):
        super(GlobalParamsModule, self).__init__(self, cap_tbl)
        self.results = self.params # this is a reference copy, so any entries in params will also appear in results
    def runmod(self):
        pass                    # nothing to do because all we want to return is a copy of the params array

## class for the module that actually runs gcam
## params: 
##   exe     = full path to gcam.exe
##   cfg     = full path to gcam configuration file
##   logcfg  = full path to gcam log configuration file
##   clobber = flag: True = clobber old outputs, False = preserve old outputs
## results:
##   dbxml   - gcam dbxml output file.  We get this from the gcam config file.    
class GcamModule(GcamModuleBase):
    def __init__(self, cap_tbl):
        super(GcamModule,self).__init__(self, cap_tbl)
        cap_tbl["gcam-core"] = self

    def runmod(self):
        ### Process the parameters
        exe    = self.params["gcam.exe"]
        cfg    = self.params["gcam.config"]
        logcfg = self.params["gcam.logconfig"]

        ## usually the exe, cfg, and logcfg files will be in the same
        ## directory, but in case of difference, take the location of
        ## the config file as controlling.
        self.workdir = os.path.dirname(cfg)

        msgpfx = "GcamModule: "    # prefix for messages coming out of this module
        ## Do some basic checks:  do these files exist, etc.
        if !os.path.exists(exe):
            raise RuntimeError(msgpfx + "File " + exe + " does not exist!")
        if !os.path.exists(cfg):
            raise RuntimeError(msgpfx + "File " + cfg + " does not exist!")
        if !os.path.exists(logcfg):
            raise RuntimeError(msgpfx + "File " + logcfg + " does not exist!")

        ## we also need to get the location of the dbxml output file.
        ## It's in the gcam.config file (we don't repeat it in the
        ## config for this module because then we would have no way to
        ## ensure consistency).
        dbxmlfpat = re.compile(r'<Value name="dbFileName">(.*)</Value>')
        dbenabledpat = re.compile(r'<Value name="write-xml-db">(.*)</Value>')
        with open(cfg, "r") as cfgfile:
            ## we don't need to parse the whole config file; all we
            ## want is to locate the name of the output file make sure
            ## the dbxml output is turned on.
            dbxmlfile = None
            for line in f:
                ## the dbxml file name will come early in the file
                match = dbxmlfpat.match(line.lstrip())
                if match:
                    dbxmlfile = match.group(1)
                    break

            ## The file spec is a relative path, starting from the
            ## directory that contains the config file.
            dbxmlfile = os.path.join(self.workdir,dbxmlfile) 
            self.results["dbxml"] = dbxmlfile # This is our eventual output
            if os.path.exists(dbxmlfile) and not self.clobber:
                ## This is not an error; it just means we can leave
                ## the existing output in place and return it.
                self.results["changed"] = 0 # mark the cached results as clean
                return 0

            ## now make sure that the dbxml output is turned on
            for line in f:
                match = dbenabledpat.match(line.lstrip())
                if match:
                    if match.group(1) != "1":
                        raise RuntimeError(msgpfx + "Config file has dbxml input turned off.  Running GCAM would be futile.")
                    else:
                        break

        ## now we're ready to actually do the run.  We don't check the return code; we let the run() method do that.
        os.chdir(self.workdir)
        return subprocess.call([exe, '-C'+cfg, '-L'+logcfg])

## class for the hydrology code
## params:
##   workdir - working directory
##       gcm - GCM outputs to use
##  scenario - tag indicating the scenario (used for naming the output files)
##   logfile - file to direct the matlab code's output to    
##
## results: 
##   qoutfile - runoff grid (matlab format)
## flxoutfile - stream flow grid (matlab)
##     cqfile - runoff grid (c format)
##   cflxfile - stream flow grid (c format)

### This is how you run the hydrology code from the command line:
### matlab -nodisplay -nosplash -nodesktop -r "run_future_hydro('<gcm>','<scenario>');exit" > & outputs/pcm-a1-out.txt < /dev/null
class HydroModule(GcamModuleBase):
    def __init__(self, cap_tbl):
        super(HydroModule, self).__init__(self, cap_tbl)
        cap_tbl["gcam-hydro"] = self

    def runmod(self):
        workdir  = self.params["workdir"]
        gcm      = self.params["gcm"]
        scenario = self.params["scenario"]
        logfile  = self.params["logfile"]
        
        os.chdir(workdir)
        
        ## we need to check existence of input and output files
        prefile  = 'inputs/climatefut/data_' + gcm + '_' + scenario + '_pre.csv'
        tempfile = 'inputs/climatefut/data_' + gcm + '_' + scenario + '_tmp.csv'
        dtrfile  = 'inputs/climatefut/data_' + gcm + '_' + scenario + '_dtr.csv'
    
        msgpfx = "HydroModule:  "
        if not os.path.exists(prefile):
            raise RuntimeError(msgpfx + "missing input file: " + prefile)
        if not os.path.exists(tempfile):
            raise RuntimeError(msgpfx + "missing input file: " + tempfile)
        if not os.path.exists(dtrfile):
            raise RuntimeError(msgpfx + "missing input file: " + dtrfile)

        ## matlab files for future processing steps
        qoutfile   = 'outputs/Avg_Runoff_235_' + gcm + '_' + scenario + '.mat'
        flxoutfile = 'outputs/Avg_ChFlow_235_' + gcm + '_' + scenario + '.mat'
        ## c-data files for final output
        cqfile   = 'outputs/cdata/runoff_' + gcm + '_' + scenario + '.mat'
        cflxfile = 'outputs/cdata/chflow_' + gcm + '_' + scenario + '.mat'

        ## Our result is the location of these output files.  Set that
        ## now, even though the files won't be created until we're
        ## done running.
        results['qoutfile']   = qoutfile
        results['flxoutfile'] = flxoutfile
        results['cqfile']     = cqfile 
        results['cflxfile']   = cflxfile

        if not self.clobber: 
            allfiles = 1
            for file in [qoutfile, flxoutfile, cqfile, cflxfile]:
                if not os.path.exists(file):
                    allfiles = 0
                    break
            if allfiles:
                ## all files exist, and we don't want to clobber them
                self.results["changed"] = 0 # mark cached results as clean
                return 0

        ## Run the matlab code.
        ## TODO: eventually we need to move away from matlab, as it is not a
        ##       suitable batch language.  Notably, if it encounters an error
        ##       it will stop at a command prompt instead of exiting with an
        ##       error code.  Yuck.
        with open(logfile,"w") as logdata, open("/dev/null", "r") as null:
            arglist = ['matlab', '-nodisplay', '-nosplash', '-nodesktop', '-r', "run_future_hydro('" + gcm+ "','" + "a1');exit"]
            sp = subprocess.Popen(arglist, stdin=null, stdout=logdata, stderr=subprocess.STDOUT)
            return sp.wait()
    ## end of runmod()
        
## class for the water disaggregation code
## params
##    workdir  - working directory
##  outputdir  - directory for outputs
##   scenario  - scenario tag
##
## results:  c-style binary files for each of the following variables
##           (the key is the variable name; the value is the filename):
##           "wdtotal", "wddom", "wdelec", "wdirr", "wdliv", "wdmanuf", "wdmining", "wsi"
        
### This is how you run the disaggregation code
### matlab -nodisplay -nosplash -nodesktop -r "run_disaggregation('<runoff-file>', '<chflow-file>', '<gcam-filestem>');exit" >& <logfile> < /dev/null
class WaterDisaggregationModule(GcamModuleBase):
    def __init__(self, depends):
        super(WaterDisaggregationModule, self, cap_tbl).__init__(self, cap_tbl)
        cap_tbl["water-disaggregation"] = self

    def runmod(self):

        workdir  = self.params["workdir"]
        os.chdir(workdir)

        hydro_rslts = self.cap_tbl["gcam-hydro"].fetch() # hydrology module
        gcam_rslts  = self.cap_tbl["gcam-water"].fetch() # gcam water outputs module

        runoff_file   = hydro_rslts["qoutfile"]
        chflow_file   = hydro_rslts["flxoutfile"]
        gcam_filestem = gcam_rslts["gcam-filestem"]
        workdir       = self.params["workdir"]
        outputdir     = self.params["outputdir"]
        scenariotag   = self.params["scenario"]

        vars = ["wdtotal", "wddom", "wdelec", "wdirr", "wdliv", "wdmanuf", "wdmining", "wsi"]
        allfiles = 1
        for var in vars:
            filename = "%s/%s-%s.dat" % (outputdir, var, scenariotag)
            self.results[var] = filename
            if not os.path.exists(filename):
                allfiles = 0
                break

        if allfiles and not clobber and not (gcam_rslts["changed"] or hydro_rslts["changed"]):
            self.results["changed"] = 0
            return 0

        ## Run the disaggregation model 
        with open(self.params["logfile"],"w") as logdata, open("/dev/null","r") as null:
            arglist = ["matlab", "-nodisplay", "-nosplash", "-nodesktop", "-r",
                       "run_downscaling('%s', '%s', '%s', '%s', '%s'" % (runoff_file, chflow_file, gcam_filestem, outputdir, scenariotag)]
                       
            sp = subprocess.Popen(arglist, stdin=null, stdout=logdata, stderr=subprocess.STDOUT) 
            return sp.wait()
        
    ## end of runmod
        
