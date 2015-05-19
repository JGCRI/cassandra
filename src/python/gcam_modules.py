import os
import os.path
import re
import subprocess
import threading
import tempfile
from sys import stdout
from sys import stderr
import gcamutil
from gcamutil import *

##### Definitions for the modules for GCAM and associated downstream models

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
####           between the modules.  This method returns the thread
####           object, mainly so that the driver can call join() on
####           all of the module threads. 
####           TODO: implement an active thread counter.

#### runmod_wrapper(): used internally by run().  Don't monkey around
####           with this function.

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
####           be called from the runmod_wrapper() method via the
####           run() method.  Together, these methods perform the
####           additional bookkeeping required to ensure that modules
####           don't try to use results before they are ready.


#### Attributes:

####  params: dictionary of parameters parsed from the config file.
####          Generally this array should be altered only by calling
####          the addparam method.

class GcamModuleBase(object):
    def __init__(self, cap_tbl):
        self.status = 0         # status indicator: 0- not yet run, 1- complete, 2- error
        self.results = {}
        self.params  = {}
        self.results["changed"] = 1
        self.cap_tbl = cap_tbl # store a reference to the capability lookup table
        self.condition = threading.Condition()

    def run(self):
        ## TODO: have this run in a separate thread
        thread = threading.Thread(target=lambda: self.runmod_wrapper())
        thread.start()
        ## returns immediately
        return thread

    ## This wrapper locks the condition variable, executes the runmod
    ## function, and unlocks when the runmod function returns.  The
    ## way we currently have it implemented, if there is some sort of
    ## error, we don't set the complete variable or notify waiting
    ## threads.  This will cause a deadlock.  We should handle this
    ## more gracefully.  This function should only ever be called from
    ## the run() method above.  <- XXX this problem should be fixed
    ## now, but we haven't tested the fix.
    def runmod_wrapper(self):
        with self.condition:
            try:
                rv = self.runmod()
                if not rv==0:
                    ## possibly add some other error handling here.
                    raise RuntimeError("%s:  runmod returned error code %s" % (self.__class__, str(rv)))
                else:
                    stdout.write("%s: finished successfully.\n"%(self.__class__))

                self.status = 1                  # set success condition
            except:
                self.status = 2                  # set error condition
                raise
            finally:
                self.condition.notify_all()      # release any waiting threads
        ## end of with block:  lock on condition var released.
        
    def fetch(self):
        ## get the results of the calculation.  These aren't returned
        ## from run() because it will run asynchronously.  This method
        ## waits if necessary and returns the results.
        with self.condition: 
            if self.status == 0:                  # module hasn't run yet.  Wait on it
                print "\twaiting on %s\n" % self.__class__
                self.condition.wait()
        ## end of with block:  lock is released

        ## By this point, the module should have run.  If status is not success, then
        ## there has been an error.
        if self.status != 1:
            raise RuntimeError("%s: wait() returned with non-success status!"%self.__class__)

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

    def finish(self):
        ## acquire a lock and release at the end of the block
        with self.condition:
            self.complete = 1
            self.condition.notify_all()
        ## end of with block:  lock is released

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
        super(GlobalParamsModule, self).__init__(cap_tbl)
        self.results = self.params # this is a reference copy, so any entries in params will also appear in results
        print self.results
        cap_tbl["general"] = self

        ## We need to allow gcamutil access to thiese parameters, since it doesn't otherwise know how to find the
        ## global params module.  
        gcamutil.genparams = self.params
        self.complete = 1       # nothing to do, so we're always complete
        
    def runmod(self):
        return 0                # nothing to do here.

    
## class for the module that actually runs gcam
## params: 
##   exe        = full path to gcam.exe
##   config     = full path to gcam configuration file
##   logconfig  = full path to gcam log configuration file
##   clobber    = flag: True = clobber old outputs, False = preserve old outputs
## results:
##   dbxml   - gcam dbxml output file.  We get this from the gcam config file.    
class GcamModule(GcamModuleBase):
    def __init__(self, cap_tbl):
        super(GcamModule,self).__init__(cap_tbl)
        cap_tbl["gcam-core"] = self

    def runmod(self):
        ### Process the parameters
        exe    = self.params["exe"]
        cfg    = self.params["config"]
        logcfg = self.params["logconfig"]
        try:
            logfile = self.params['logfile'] # file for redirecting gcam's copious stdout
        except KeyError:
            ## logfile is optional
            logfile = None

        ## usually the exe, cfg, and logcfg files will be in the same
        ## directory, but in case of difference, take the location of
        ## the config file as controlling.
        self.workdir = os.path.dirname(cfg)

        msgpfx = "GcamModule: "    # prefix for messages coming out of this module
        ## Do some basic checks:  do these files exist, etc.
        if not os.path.exists(exe):
            raise RuntimeError(msgpfx + "File " + exe + " does not exist!")
        if not os.path.exists(cfg):
            raise RuntimeError(msgpfx + "File " + cfg + " does not exist!")
        if not os.path.exists(logcfg):
            raise RuntimeError(msgpfx + "File " + logcfg + " does not exist!")

        ## we also need to get the location of the dbxml output file.
        ## It's in the gcam.config file (we don't repeat it in the
        ## config for this module because then we would have no way to
        ## ensure consistency).
        dbxmlfpat = re.compile(r'<Value name="xmldb-location">(.*)</Value>')
        dbenabledpat = re.compile(r'<Value name="write-xml-db">(.*)</Value>')
        with open(cfg, "r") as cfgfile:
            ## we don't need to parse the whole config file; all we
            ## want is to locate the name of the output file make sure
            ## the dbxml output is turned on.
            dbxmlfile = None
            for line in cfgfile:
                ## the dbxml file name will come early in the file
                match = dbxmlfpat.match(line.lstrip())
                if match:
                    dbxmlfile = match.group(1)
                    break

            print "%s:  dbxmlfile = %s" % (self.__class__, dbxmlfile)
            ## The file spec is a relative path, starting from the
            ## directory that contains the config file.
            dbxmlfile = os.path.join(self.workdir,dbxmlfile) 
            self.results["dbxml"] = dbxmlfile # This is our eventual output
            if os.path.exists(dbxmlfile):
                if not self.clobber:
                    ## This is not an error; it just means we can leave
                    ## the existing output in place and return it.
                    print "GcamModule:  results exist and no clobber.  Skipping."
                    self.results["changed"] = 0 # mark the cached results as clean
                    return 0
                else:
                    ## have to remove the dbxml, or we will merely append to it
                    os.unlink(dbxmlfile)

            ## now make sure that the dbxml output is turned on
            for line in cfgfile:
                match = dbenabledpat.match(line.lstrip())
                if match:
                    if match.group(1) != "1":
                        raise RuntimeError(msgpfx + "Config file has dbxml input turned off.  Running GCAM would be futile.")
                    else:
                        break

        ## now we're ready to actually do the run.  We don't check the return code; we let the run() method do that.
        os.chdir(self.workdir)
        print "Running:  %s -C%s -L%s" % (exe, cfg, logcfg)

        if logfile is None:
            return subprocess.call([exe, '-C'+cfg, '-L'+logcfg])
        else:
            with open(logfile,"w") as lf:
                return subprocess.call([exe, '-C'+cfg, '-L'+logcfg], stdout=lf)

## class for the hydrology code
## params:
##   workdir - working directory
##       gcm - GCM outputs to use
##  scenario - tag indicating the scenario to use.
##     runid - tag indicating which ensemble member to use.
##   logfile - file to direct the matlab code's output to
## startmonth- month of year for first month in dataset. 1=Jan, 2=Feb, etc.  (OPTIONAL)            
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
        super(HydroModule, self).__init__(cap_tbl)
        cap_tbl["gcam-hydro"] = self

    def runmod(self):
        workdir  = self.params["workdir"]
        inputdir = self.params["inputdir"] # input data from GCM
        outputdir = self.params["outputdir"] # destination for output files
        gcm      = self.params["gcm"]
        scenario = self.params["scenario"]
        runid    = self.params["runid"] # identifier for the GCM ensemble member
        logfile  = self.params["logfile"]
        try:
            startmonth = int(self.params['startmonth'])
        except KeyError:
            startmonth = 1      # Default is to start at the beginning of the year
        print '[HydroModule]: start month = %d' % startmonth

        ## get initial channel storage from historical hydrology
        ## module if available, or from self-parameters if not
        ## XXX Should the self-parameters override the module or vice versa?
        if self.cap_tbl.has_key('historical-hydro'):
            hist_rslts = self.cap_tbl['historical-hydro'].fetch()
            initstorage = hist_rslts['chstorfile']
        else:
            initstorage = self.params["init-storage-file"] # matlab data file containing initial storage

        
        os.chdir(workdir)

        if inputdir[-1] != '/':
            inputdir = inputdir + '/'
        if outputdir[-1] != '/':
            outputdir = outputdir + '/'
        
        ## we need to check existence of input and output files
        prefile  = inputdir + 'pr_Amon_' + gcm + '_' + scenario + '_' + runid + '.mat'
        tempfile = inputdir + 'tas_Amon_' + gcm + '_' + scenario + '_' + runid + '.mat'
        dtrfile  = inputdir + 'dtr_Amon_' + gcm + '_' + scenario + '_' + runid + '.mat'

        print "input files:\n\t%s\n\t%s\n\t%s" % (prefile, tempfile, dtrfile)
    
        msgpfx = "HydroModule:  "
        if not os.path.exists(prefile):
            raise RuntimeError(msgpfx + "missing input file: " + prefile)
        if not os.path.exists(tempfile):
            raise RuntimeError(msgpfx + "missing input file: " + tempfile)
        if not os.path.exists(dtrfile):
            raise RuntimeError(msgpfx + "missing input file: " + dtrfile)

        ## filename bases
        qoutbase = outputdir + 'Avg_Runoff_235_' + gcm + '_' + scenario + '_' + runid
        foutbase = outputdir + 'Avg_ChFlow_235_' + gcm + '_' + scenario + '_' + runid
        boutbase = outputdir + 'basin_runoff_235_' + gcm + '_' + scenario + '_' + runid
        routbase = outputdir + 'rgn_runoff_235_'+gcm+'_' + scenario + '_' + runid
        petoutbase = outputdir + 'Avg_PET_235_' + gcm + '_' + scenario + '_' + runid
        
        ## matlab files for future processing steps 
        qoutfile   = qoutbase + '.mat'
        foutfile   = foutbase + '.mat'
        basinqfile = boutbase + '.mat'
        rgnqfile   = routbase + '.mat'
        petoutfile = petoutbase + '.mat'
        ## c-data files for final output
        cqfile     = qoutbase + '.dat'
        cflxfile   = foutbase + '.dat'
        cbasinqfile = boutbase + '.dat'
        crgnqfile  = routbase + '.dat'
        ## csv tables for diagnostics
        basinqtblfile  = boutbase + '.csv'
        rgnqtblfile    = routbase + '.csv'
        
        ## Our result is the location of these output files.  Set that
        ## now, even though the files won't be created until we're
        ## done running.
        self.results['qoutfile']   = qoutfile
        self.results['foutfile']   = foutfile
        self.results['cqfile']     = cqfile 
        self.results['cflxfile']   = cflxfile
        self.results['basinqfile'] = basinqfile
        self.results['cbasinqfile'] = cbasinqfile
        self.results['rgnqfile']   = rgnqfile
        self.results['crgnqfile']  = crgnqfile
        self.results['basinqtbl']  = basinqtblfile
        self.results['rgnqtbl']    = rgnqtblfile
        self.results['petoutfile'] = petoutfile
        
        ## We need to report the runid so that other modules that use
        ## this output can name their files correctly.
        self.results['runid']      = runid

        alloutfiles = [qoutfile, foutfile, cqfile, cflxfile, basinqfile, cbasinqfile,
                       rgnqfile, crgnqfile, basinqtblfile, rgnqtblfile, petoutfile]
        if not self.clobber and gcamutil.allexist(alloutfiles):
            ## all files exist, and we don't want to clobber them
            print "[HydroModule]:  results exist and no clobber.  Skipping."
            self.results["changed"] = 0 # mark cached results as clean
            return 0        # success code

        ## Run the matlab code.
        ## TODO: eventually we need to move away from matlab, as it is not a
        ##       suitable batch language.  Notably, if it encounters an error
        ##       it will stop at a command prompt instead of exiting with an
        ##       error code.  Yuck.

        ## Note that unlike the historical version, we don't have to
        ## pass the names of the basin and region table files, since
        ## the code can infer them from the basinqfile and rgnqfile
        ## parameters. 
        ## TODO: prune the number of filenames passed by inferring all
        ## of the cfoofile filenames the same way.
        print 'Running the matlab hydrology code'
        with open(logfile,"w") as logdata, open("/dev/null", "r") as null:
            arglist = ['matlab', '-nodisplay', '-nosplash', '-nodesktop', '-r',
                       "run_future_hydro('%s','%s','%s','%s', %d, '%s','%s','%s', '%s','%s');exit" %
                       (prefile,tempfile,dtrfile,initstorage, startmonth, qoutfile,foutfile,petoutfile, basinqfile,rgnqfile)]
            sp = subprocess.Popen(arglist, stdin=null, stdout=logdata, stderr=subprocess.STDOUT)
            rc = sp.wait()
        ## matlab often won't return an error code when it fails, so check to see that all files were created
        if gcamutil.allexist(alloutfiles):
            return rc
        else:
            stderr.write('[HydroModule]: Some output files missing.  Check logfile (%s) for more information\n'%logfile)
            return 1            # nonzero return code indicates failure
    ## end of runmod()


## class for historical hydrology run.  This is similar to, but not
## quite the same as, the main hydro module.
## params:
## workdir  - working directory for the matlab runs
## inputdir - location of the input files    
##  gcm     - Which GCM to use (each has its own historical data)
## runid    - Tag indicating the run-id (e.g.  r1i1p1_195001_200512 )
## outputdir- Destination directory for output    
## logfile  - file to redirect matlab output to
## startmonth- month of year for first month in dataset (OPTIONAL)
##
## results:
##   qoutfile - runoff grid (matlab format)
## flxoutfile - stream flow grid (matlab format)
## chstorfile - channel storage grid (matlab format)
class HistoricalHydroModule(GcamModuleBase):
    def __init__(self, cap_tbl):
        super(HistoricalHydroModule, self).__init__(cap_tbl)
        cap_tbl['historical-hydro'] = self

    def runmod(self):
        workdir   = self.params['workdir']
        inputdir  = self.params['inputdir'] 
        outputdir = self.params['outputdir']
        gcm       = self.params['gcm']
        scenario  = 'historical'
        runid     = self.params['runid']
        logfile   = self.params['logfile']
        try:
            startmonth = int(self.params['startmonth'])
        except KeyError:
            startmonth = 1      # Default is January
        print '[HistoricalHydroModule]: start month = %d' % startmonth

        os.chdir(workdir)

        if inputdir[-1] != '/':
            inputdir = inputdir + '/'
        if outputdir[-1] != '/':
            outputdir = outputdir + '/'
        
        ## we need to check existence of input and output files
        prefile  = inputdir + 'pr_Amon_' + gcm + '_' + scenario + '_' + runid + '.mat'
        tempfile = inputdir + 'tas_Amon_' + gcm + '_' + scenario + '_' + runid + '.mat'
        dtrfile  = inputdir + 'dtr_Amon_' + gcm + '_' + scenario + '_' + runid + '.mat'

        print "input files:\n\t%s\n\t%s\n\t%s" % (prefile, tempfile, dtrfile)

        msgpfx = "HistoricalHydroModule:  "
        if not os.path.exists(prefile):
            raise RuntimeError(msgpfx + "missing input file: " + prefile)
        if not os.path.exists(tempfile):
            raise RuntimeError(msgpfx + "missing input file: " + tempfile)
        if not os.path.exists(dtrfile):
            raise RuntimeError(msgpfx + "missing input file: " + dtrfile)

        ## output filenames
        qoutfile      = outputdir + 'Avg_Runoff_235_' + gcm + '_' + scenario + '_' + runid + '.mat'
        petoutfile    = outputdir + 'Avg_PET_235_' + gcm + '_' + scenario + '_' + runid + '.mat'
        basinqtblfile = outputdir + 'basin_runoff_235_' + gcm + '_' + scenario + '_' + runid + '.csv'
        rgnqtblfile   = outputdir + 'rgn_runoff_235_' + gcm + '_' + scenario + '_' + runid + '.csv'        
        foutfile      = outputdir + 'Avg_ChFlow_235_' + gcm + '_' + scenario + '_' + runid + '.mat'
        chstorfile    = outputdir + 'InitChStor_' + gcm + '_' + scenario + '_' + runid + '.mat'

        ## Results will be these file names.  Set up the results
        ## entries now, even though the files won't be ready yet.
        self.results['qoutfile'] = qoutfile
        self.results['foutfile'] = foutfile
        self.results['chstorfile'] = chstorfile
        self.results['basinqtbl']  = basinqtblfile
        self.results['rgnqtbl']    = rgnqtblfile
        self.results['petoutfile'] = petoutfile

        ## Test to see if the outputs already exist.  If so, then we can skip these calcs.
        alloutfiles = [qoutfile, foutfile, petoutfile, chstorfile, basinqtblfile, rgnqtblfile]
        if not self.clobber and gcamutil.allexist(alloutfiles):
            print "[HistoricalHydroModule]: results exist and no clobber set.  Skipping."
            self.results['changed'] = 0
            return 0        # success code

        ## If we get here, then we need to run the historical
        ## hydrology.  Same comments apply as to the regular hydrology
        ## module.
        print 'Running historical hydrology for gcm= %s   runid= %s' % (gcm, runid)
        with open(logfile,'w') as logdata, open('/dev/null','r') as null:
            arglist = ['matlab', '-nodisplay', '-nosplash', '-nodesktop', '-r',
                       "run_historical_hydro('%s', '%s', '%s', %d, '%s', '%s','%s', '%s', '%s', '%s');exit" %
                       (prefile, tempfile, dtrfile, 1, chstorfile, qoutfile, foutfile,petoutfile, basinqtblfile, rgnqtblfile)]
            sp = subprocess.Popen(arglist, stdin=null, stdout=logdata, stderr=subprocess.STDOUT)
            rc = sp.wait()
        ## check to see if the outputs were actually created; matlab will sometimes fail silently
        if gcamutil.allexist(alloutfiles):
            return rc
        else:
            stderr.write('[HistoricalHydroModule]: Some output files were not created.  Check logfile (%s) for details.\n'%logfile)
            return 1            # nonzero indicates failure
    
## class for the water disaggregation code
## params
##    workdir  - working directory
##  outputdir  - directory for outputs
##   inputdir  - directory for static inputs
##   scenario  - scenario tag
##     rgnmap  - region mapping file
##
## results:  c-style binary files for each of the following variables
##           (the key is the variable name; the value is the filename):
##           "wdtotal", "wddom", "wdelec", "wdirr", "wdliv", "wdmanuf", "wdmining", "wsi"
        
### This is how you run the disaggregation code
### matlab -nodisplay -nosplash -nodesktop -r "run_disaggregation('<runoff-file>', '<chflow-file>', '<gcam-filestem>');exit" >& <logfile> < /dev/null
class WaterDisaggregationModule(GcamModuleBase):
    def __init__(self, cap_tbl):
        super(WaterDisaggregationModule, self).__init__(cap_tbl)
        cap_tbl["water-disaggregation"] = self

    def runmod(self):
        import waterdisag

        workdir  = self.params["workdir"]
        os.chdir(workdir)

        hydro_rslts = self.cap_tbl["gcam-hydro"].fetch() # hydrology module
        gcam_rslts  = self.cap_tbl["gcam-core"].fetch() # gcam core module

        runoff_file   = hydro_rslts["qoutfile"]
        chflow_file   = hydro_rslts["foutfile"]
        basinqfile    = hydro_rslts["basinqfile"]
        rgnqfile      = hydro_rslts["rgnqfile"]
        runid         = hydro_rslts["runid"]
        dbxmlfile     = gcam_rslts["dbxml"]
        #dbxmlfile     = "/lustre/data/rpl/gcam-water/SSP_Scen0.dbxml"
        outputdir     = self.params["outputdir"]
        tempdir       = self.params["tempdir"]  # location for intermediate files produced by dbxml queries
        inputdir      = self.params["inputdir"] # static inputs, such as irrigation share and query files.
        scenariotag   = self.params["scenario"]
        if self.params.has_key('rgnmap'):
            rgnmap = self.params['rgnmap']
        else:
            stdout.write('[WaterDisaggregationModule]: using default region mapping.')
            rgnmap = 'inputs/newgrd_GCAM.csv'
        ## Parse the water transfer parameters.
        if self.params.has_key('water-transfer'):
            transfer      = gcamutil.parseTFstring(self.params['water-transfer'])
            try:
                transfer_file = self.params['transfer-file']
            except KeyError:
                stderr.write('Water transfer set, but no transfer data file specified.\n')
                return 5
        else:
            transfer = False
            transfer_file = '/dev/null' # won't be used by the matlab program, but we still need a placeholder

        self.results['water-transfer'] = transfer
        ## append the transfer status to the scenario tag
        if transfer:
            scenariotag = scenariotag + 'wT'
        else:
            scenariotag = scenariotag + 'wF' 
        print 'scenariotag = %s' % scenariotag

        ## Helper function generator
        def get_dir_prepender(dir):
            if dir[-1]=='/':
                return lambda file: dir+file
            else:
                return lambda file: dir+'/'+file

        inputdirprep  = get_dir_prepender(inputdir)
        tempdirprep = get_dir_prepender(tempdir)
        outdirprep  = get_dir_prepender(outputdir) 
        
        vars = ["wdtotal", "wddom", "wdelec", "wdirr", "wdliv", "wdmfg", "wdmin", "wsi",
                "basin-supply", "basin-wdtot", "basin-wddom", "basin-wdelec", "basin-wdirr", "basin-wdliv", "basin-wdmfg", "basin-wdmin", "basin-wsi",
                "rgn-supply", "rgn-wdtot", "rgn-wddom", "rgn-wdelec", "rgn-wdirr", "rgn-wdliv", "rgn-wdmfg", "rgn-wdmin", "rgn-wsi"]
        allfiles = 1
        for var in vars:
            filename = "%s/%s-%s-%s.dat" % (outputdir, var, scenariotag, runid)
            self.results[var] = filename
            if not os.path.exists(filename):
                print 'File %s does not exist.  Running WaterDisaggregationModule.\n' % filename
                allfiles = 0

        
        pop_demo_file = outdirprep("pop-demo.csv") # changed this to use the same region ordering in the water data.
        self.results['pop-demo'] = pop_demo_file

        if allfiles and not self.clobber and not (gcam_rslts["changed"] or hydro_rslts["changed"]):
            print "WaterDisaggregationModule: results exist and no clobber.  Skipping."
            self.results["changed"] = 0
            return 0


        print 'disaggregation results:\n%s' % str(self.results)
        
        queryfiles = ['batch-land-alloc.xml', 'batch-population.xml', 'batch-water-ag.xml',
                      'batch-water-dom.xml', 'batch-water-elec.xml', 'batch-water-livestock.xml',
                      'batch-water-mfg.xml', 'batch-water-mining.xml']
        outfiles   = ['batch-land-alloc.csv', 'batch-population.csv', 'batch-water-ag.csv',
                      'batch-water-dom.csv', 'batch-water-elec.csv', 'batch-water-livestock.csv',
                      'batch-water-mfg.csv', 'batch-water-mining.csv']
        queryfiles = map(inputdirprep, queryfiles)
        outfiles = map(tempdirprep, outfiles)
        gcamutil.gcam_query(queryfiles, dbxmlfile, outfiles)

        ### reformat the GCAM outputs into the files the matlab code needs 
        ### note all the csv files referred to here are temporary
        ### files.  On the input side the names need to match the ones
        ### used in the configuration of the gcam model interface
        ### queries, and on the output side they must match the ones
        ### used in the matlab disaggregation code.

        ## non-ag demands (sadly, I didn't think to put the lists
        ## above in the order we were planning to use them.)
        wddom   = waterdisag.proc_wdnonag(outfiles[3], tempdirprep("withd_dom.csv"))
        wdelec  = waterdisag.proc_wdnonag(outfiles[4], tempdirprep("withd_elec.csv"))
        wdman   = waterdisag.proc_wdnonag(outfiles[6], tempdirprep("withd_mfg.csv"))
        wdmin   = waterdisag.proc_wdnonag(outfiles[7], tempdirprep("withd_min.csv"))
        wdnonag = waterdisag.proc_wdnonag_total(tempdirprep("withd_nonAg.csv"), wddom, wdelec, wdman, wdmin)

        ## population data
        waterdisag.proc_pop(outfiles[1], tempdirprep("pop_fac.csv"), tempdirprep("pop_tot.csv"), pop_demo_file)

        ## livestock demands
        wdliv  = waterdisag.proc_wdlivestock(outfiles[5], tempdirprep("withd_liv.csv"), tempdirprep('rgn_tot_withd_liv.csv'))

        ## agricultural demands and auxiliary quantities
        waterdisag.proc_irr_share(inputdirprep('irrigation-frac.csv'), tempdirprep("irrS.csv"))
        waterdisag.proc_ag_area(outfiles[0], tempdirprep("irrA.csv"))
        waterdisag.proc_ag_vol(outfiles[2], tempdirprep("withd_irrV.csv"))

        ## Run the disaggregation model
        if transfer:
            tflag = 1
        else:
            tflag = 0
        matlabfn = "run_disaggregation('%s','%s','%s','%s', '%s', '%s','%s','%s', '%s', %s, '%s');" % (runoff_file, chflow_file,basinqfile,rgnqfile,  rgnmap, tempdir, outputdir, scenariotag,runid, tflag, transfer_file)
        print 'current dir: %s ' % os.getcwd()
        print 'matlab fn:  %s' % matlabfn
        with open(self.params["logfile"],"w") as logdata, open("/dev/null","r") as null:
            arglist = ["matlab", "-nodisplay", "-nosplash", "-nodesktop", "-r", matlabfn]

            sp = subprocess.Popen(arglist, stdin=null, stdout=logdata, stderr=subprocess.STDOUT) 
            return sp.wait()
        
    ## end of runmod
        
## class for the netcdf-demo builder
## params:
##   mat2nc  - location of the netcdf converter executable
##     dsid  - dataset id
##  forcing  - forcing value (written into the output data as metadata)
## globalpop - 2050 global population (written into output data as metadata)
##    pcGDP  - 2050 per-capita GDP (written into output data as metadata -- currently not used anyhow)
## outputdir - output directory
class NetcdfDemoModule(GcamModuleBase):
    def __init__(self, cap_tbl):
        super(NetcdfDemoModule, self).__init__(cap_tbl)
        cap_tbl['netcdf-demo'] = self

    def runmod(self):
        hydro_rslts = self.cap_tbl['gcam-hydro'].fetch()
        water_rslts = self.cap_tbl['water-disaggregation'].fetch()

        print 'water_rslts:\n%s' % str(water_rslts)

        chflow_file  = hydro_rslts['cflxfile']
        transfer     = water_rslts['water-transfer']

        rcp = self.params['rcp']
        pop = self.params['pop']
        gdp = self.params['gdp']
        outfile = self.params['outfile']
        mat2nc  = self.params['mat2nc']
        
        self.results['outfile'] = outfile
        
        try:
            ## create a temporary file to hold the config
            (fd, tempfilename) = tempfile.mkstemp()
            cfgfile = os.fdopen(fd,"w")

            cfgfile.write('%s\n%s\n%s\n' % (rcp, pop, gdp))
            cfgfile.write('%s\n' % outfile)
            if transfer:
                cfgfile.write('/lustre/data/rpl/gcam-driver/output/cmip5/no-data.dat\n')
            else:
                cfgfile.write('%s\n' % chflow_file)
            for var in ['wdirr', 'wdliv', 'wdelec', 'wdmfg', 'wdtotal', 'wddom', 'wsi']:
                if transfer:
                    ## for water transfer cases, we don't have any gridded data, so substitute a grid full of NaN.
                    cfgfile.write('/lustre/data/rpl/gcam-driver/output/cmip5/no-data.dat\n')
                else:
                    cfgfile.write('%s\n' % water_rslts[var])
            cfgfile.write('%s\n' % water_rslts['pop-demo'])
            for var in ['basin-supply', 'basin-wdirr', 'basin-wdliv', 'basin-wdelec', 'basin-wdmfg', 'basin-wdtot', 'basin-wddom', 'basin-wsi',
                        'rgn-supply', 'rgn-wdirr', 'rgn-wdliv', 'rgn-wdelec', 'rgn-wdmfg', 'rgn-wdtot', 'rgn-wddom', 'rgn-wsi']:
                cfgfile.write('%s\n' % water_rslts[var])

            cfgfile.close()

            return subprocess.call([mat2nc, tempfilename])
        finally:
            os.unlink(tempfilename)
