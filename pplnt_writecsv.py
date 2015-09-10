import csv

#Question for Robert: how can I set a default file name according to our naming conventions?
def pplnt_writecsv(tuple_list, name = 'pplnt_data.csv'):
    """Takes list of (lon, lat, water-usage) tuples ('tuple_list') and a string indicating the
    name of the output file ('name') and writes to a 3-column csv ('outfile')."""

    outfile = open(name, 'wb')

    csv_write = csv.writer(outfile, delimiter=',')
    csv_write.writerows(tuple_list)

    outfile.close()

