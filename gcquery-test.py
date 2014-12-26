import gcamutil

gcamutil.genparams = {"ModelInterface" : "/lustre/data/rpl/ModelInterface/ModelInterface.jar",
                      "DBXMLlib" : "/homes/pralitp/libs/dbxml-2.5.16/install/lib"}

dbxml_file = "/lustre/data/rpl/gcam-4745/output/database.dbxml"

queryfiles = ['driver-forcing-batch.xml', 'driver-freight-batch.xml', 'driver-primary-batch.xml']
## add the directory path to the query files
queryfiles = map(lambda file: '/lustre/data/rpl/ModelInterface/batch-queries/' + file, queryfiles)

outfiles = ['forcing.csv', 'freight.csv', 'primary.csv']
outfiles = map(lambda file: '/lustre/data/rpl/gcam-driver/output/' + file, outfiles)

of_new = gcamutil.gcam_query(queryfiles, dbxml_file, outfiles)


print 'output is:\t', of_new


