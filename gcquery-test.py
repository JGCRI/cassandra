import gcamutil
import waterdisag

gcamutil.genparams = {"ModelInterface" : "/lustre/data/rpl/ModelInterface/ModelInterface.jar",
                      "DBXMLlib" : "/homes/pralitp/libs/dbxml-2.5.16/install/lib"}

dbxml_file = "/lustre/data/rpl/gcam-water/SSP_Scen0.dbxml"

queryfiles = ['queries/batch-land-alloc.xml', 'queries/batch-population.xml', 'queries/batch-water-ag.xml',
              'queries/batch-water-dom.xml', 'queries/batch-water-elec.xml', 'queries/batch-water-livestock.xml',
              'queries/batch-water-mfg.xml', 'queries/batch-water-mining.xml']


## add the directory path to the query files
queryfiles = map(lambda file: '/lustre/data/rpl/gcam-driver/' + file, queryfiles)

outfiles = ['batch-land-alloc.csv', 'batch-population.csv', 'batch-water-ag.csv',
              'batch-water-dom.csv', 'batch-water-elec.csv', 'batch-water-livestock.csv',
              'batch-water-mfg.csv', 'batch-water-mining.csv']

outfiles = map(lambda file: '/lustre/data/rpl/gcam-driver/output/' + file, outfiles)

of_new = gcamutil.gcam_query(queryfiles, dbxml_file, outfiles)

## process the non-ag water
wd_dom  = waterdisag.proc_wdnonag('output/batch-water-dom.csv', 'output/final_wd_dom.csv')
wd_elec = waterdisag.proc_wdnonag('output/batch-water-elec.csv', 'output/final_wd_elec.csv')
wd_mfg  = waterdisag.proc_wdnonag('output/batch-water-mfg.csv', 'output/final_wd_mfg.csv')
wd_min  = waterdisag.proc_wdnonag('output/batch-water-mining.csv', 'output/final_wd_mining.csv') 
wd_tot  = waterdisag.proc_wdnonag_total('output/final_wd_total.csv', wd_dom, wd_elec, wd_mfg, wd_min)

## process livestock water demand
wd_liv  = waterdisag.proc_wdlivestock('output/batch-water-livestock.csv', 'output/final_wd_liv.csv')



