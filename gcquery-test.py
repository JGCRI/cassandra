import gcamutil

gcamutil.genparams = {"ModelInterface" : "/lustre/data/rpl/ModelInterface/ModelInterface.jar",
                      "DBXMLlib" : "/homes/pralitp/libs/dbxml-2.5.16/install/lib"}

dbxml_file = "/lustre/data/rpl/gcam-hector/output/database.dbxml"

queryfiles = ['xmldb-forcing-batch.xml', 'xmldb-freight-batch.xml', 'xmldb-primary-batch.xml']
## add the directory path to the query files
queryfiles = map(lambda file: '/lustre/data/rpl/ModelInterface/batch-queries/' + file, queryfiles)

outfiles = ['forcing.csv', 'freight.csv', 'primary.csv']

gcamutil.gcam_query(queryfiles, dbxml_file, outfiles)


