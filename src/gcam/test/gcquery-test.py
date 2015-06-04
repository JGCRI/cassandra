import sys

## enable this code to run from the gcam/test/ directory
sys.path.append('../..')

from gcam import util
from gcam.modules import GlobalParamsModule
from gcam.water import waterdisag


## Set up the global parameters module (which is used by some of the
## utility functions).
genparams = {"ModelInterface" : "/lustre/data/rpl/ModelInterface-baseX/ModelInterface.jar",
             "DBXMLlib" : "/homes/pralitp/libs/dbxml-2.5.16/install/lib"} 
global_params = GlobalParamsModule({})
for key in genparams.keys():
    global_params.addparam(key, genparams[key])
global_params.run()

dbfile = "/lustre/data/rpl/gcam-ifam-32rgn/database_basexdb"

queryfiles = ['input-data/batch-land-alloc.xml', 'input-data/batch-population.xml', 'input-data/batch-water-ag.xml',
              'input-data/batch-water-dom.xml', 'input-data/batch-water-elec.xml', 'input-data/batch-water-livestock.xml',
              'input-data/batch-water-mfg.xml', 'input-data/batch-water-mining-alt.xml']


## add the directory path to the query files
queryfiles = map(lambda file: '/lustre/data/rpl/gcam-driver/' + file, queryfiles)

outfiles = ['batch-land-alloc.csv', 'batch-population.csv', 'batch-water-ag.csv',
              'batch-water-dom.csv', 'batch-water-elec.csv', 'batch-water-livestock.csv',
              'batch-water-mfg.csv', 'batch-water-mining.csv']

outfiles = map(lambda file: '/lustre/data/rpl/gcam-driver/output/test/' + file, outfiles)

of_new = util.gcam_query(queryfiles, dbfile, outfiles)

## Just test the queries, not the processing.
raise SystemExit

## process the non-ag water
wd_dom  = waterdisag.proc_wdnonag('output/batch-water-dom.csv', 'output/final_wd_dom.csv')
wd_elec = waterdisag.proc_wdnonag('output/batch-water-elec.csv', 'output/final_wd_elec.csv')
wd_mfg  = waterdisag.proc_wdnonag('output/batch-water-mfg.csv', 'output/final_wd_mfg.csv')
wd_min  = waterdisag.proc_wdnonag('output/batch-water-mining.csv', 'output/final_wd_mining.csv') 
wd_tot  = waterdisag.proc_wdnonag_total('output/final_wd_total.csv', wd_dom, wd_elec, wd_mfg, wd_min)

## process livestock water demand
wd_liv  = waterdisag.proc_wdlivestock('output/batch-water-livestock.csv', 'output/final_wd_liv.csv')

## process ag water demand
irrS    = waterdisag.proc_irr_share('input-data/irrigation-frac.csv', 'output/irrS.csv')
## no tables returned by these next two.  Maybe we should return them for consistency?
waterdisag.proc_ag_area('output/batch-land-alloc.csv', 'output/final_ag_area.csv')
waterdisag.proc_ag_vol('output/batch-water-ag.csv', 'output/final_wd_ag.csv')

