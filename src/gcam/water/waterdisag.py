#!/bin/env python
"""Functions supporting the water demand disaggregation module."""


from gcam import util
import re

## Canonical ordering of the regions for outputs
regions_ordered = ["USA", "Canada", "Western Europe", "Japan", "Australia_NZ",
                   "Former Soviet Union", "China", "Middle East", "Africa",
                   "Latin America", "Southeast Asia", "Eastern Europe",
                   "Korea", "India"]

## Year-2000 population benchmarks
gis2000 = {"Africa" : 813123731,
           "Australia_NZ" : 22603983,
           "Canada" : 31209299,
           "China"  : 1397857019 ,
           "Eastern Europe" : 123917239,
           "Former Soviet Union" : 297536224,
           "India" : 1010329677,
           "Japan" : 124473594,
           "Korea" : 51010641,
           "Latin America" : 508033627,
           "Middle East" : 178740493,
           "Southeast Asia" : 774664021,
           "USA" : 282494918,
           "Western Europe" : 448082681}

def rd_gcam_table(filename, njunk=0):
    """Read the data from a csv file generated as gcam output.  

    Only some GCAM tables have this format.  Right now the ones that
    do include population and non-ag water withdrawals.

    arguments:
       filename - The csv file to be read. 
          njunk - Number of junk columns after the region column to be skipped.

    """
    table = {}
    with open(filename,"r") as file:
        ## skip first two lines, which are headers
        file.readline()
        file.readline()

        for line in file:
            line = util.rm_trailing_comma(line)
            linefix = util.scenariofix(line)
            ## split off the first two columns
            linesplit = linefix.split(',',2+njunk)

            region = linesplit[1] 
            # trim the final (units) column.  Note that this will also
            # trim off the newline.
            data = (linesplit[-1].rpartition(','))[0]
                
            table[region] = data

    return table


            
## reorder the table output and write it to a file.  
def table_output_ordered(filename, table, incl_region=False, ordering=regions_ordered):
    """Reorder an output table by region and write to a file.

    arguments:
         filename - Name of intended output file.

            table - Output table to reorder (e.g. output of proc_wdnonag)

      incl_region - Flag: True = output region names in first columns;
                    False = don't.  Default: False

         ordering - List of regions in the order that they should be
                    output.  Default = canonical ordering given in
                    'regions_ordered'.

    """
    with open(filename,"w") as file:
        for region in ordering:
            if incl_region:
                file.write("%s,%s\n" % (region, table[region]))
            else:
                file.write("%s\n" % table[region])
    return

    
def proc_wdnonag(infile, outfile):
    """Process the non-agricultural water demands.

    Take the table of non-agricultural water demands output by GCAM,
    transform it to the format expected by the water demand
    disaggregation, and write to an output file.

    arguments:
       infile - Name of the input file.
      outfile - Name of the output file.

    return value: The transformed table, to be used in computing total
                  water demand.

    """
    table = rd_gcam_table(infile,2)
    table_output_ordered(outfile, table)
    return table

def proc_wdnonag_total(outfile, wddom, wdelec, wdmanuf, wdmining):
    """Sum the non-agricultural water demands to get total non-ag demand.

    arguments:
        outfile - Name of output file
          wddom - Domestic water demand table
         wdelec - Electric generation water demand table
        wdmanuf - Manufacturing water demand table
       wdmining - Mining water demand table

    return value: Total non-ag water demand table.

    """
    wdnonag = {}
    for region in regions_ordered:
        dom    = map(float, wddom[region].split(','))
        elec   = map(float, wdelec[region].split(','))
        manuf  = map(float, wdmanuf[region].split(','))
        mining = map(float, wdmining[region].split(','))

        tot = map(lambda d,e,ma,mi: d+e+ma+mi , dom, elec, manuf, mining)
        wdnonag[region] = ','.join(map(str,tot))
    table_output_ordered(outfile, wdnonag)
    return wdnonag 
    
def proc_pop(infile, outfile_fac, outfile_tot, outfile_demo):
    """Process GCAM population output.

    arguments:
          infile - Input data file.
     outfile_fac - Output file for the "pop_fac" table
     outfile_tot - Output file for the "pop_tot" table 
    outfile_demo - Output file for the reordered version of the
                   "pop_tot" table. (TODO: This output is probably
                   obsolete.)

    """
    poptbl = rd_gcam_table(infile,1)
    pop_fac = {}
    pop_tot = {}

    for region in poptbl.keys():
        popvals   = poptbl[region].split(',')
        benchmark = gis2000[region]
        fpop      = map(lambda x: str(1000*float(x)/benchmark), popvals)
        totpop    = map(lambda x: str(1000*float(x)), popvals)

        pop_fac[region] = ','.join(fpop)
        pop_tot[region] = ','.join(totpop)

    table_output_ordered(outfile_fac, pop_fac)
    table_output_ordered(outfile_tot, pop_tot)
    table_output_ordered(outfile_demo, pop_tot) # demo output now uses the normal ordering.

    return (pop_fac, pop_tot) 



## table giving the fraction of beef that is buffalo (vs. cattle), by region
bfracFAO2005 = {
    "USA"                 :	0,
    "Canada"              :	0,
    "Western Europe"      :	0.0337915515,
    "Japan"               :	0,
    "Australia_NZ"        :	0.0001073395,
    "Former Soviet Union" :	0.0235449617,
    "China"               :	0.170404984,
    "Middle East"         :	0.0326917861,
    "Africa"              :	0,
    "Latin America"       :	0,
    "Southeast Asia"      :	0.3139622207,
    "Eastern Europe"      :	0.0410626023,
    "Korea"               :	0,
    "India"               :	0.3197864781
}

## table giving the fraction of SheepGoat that is goat (vs. sheep), by region
gfracFAO2005 = {
    "USA"                 :    0.2058911719,
    "Canada"              :    0.0304360953,
    "Western Europe"      :    0.1252789991,
    "Japan"               :    0.7580543033,
    "Australia_NZ"        :    0.0043487698,
    "Former Soviet Union" :    0.1457585163,
    "China"               :    0.5365833204,
    "Middle East"         :    0.3013901891,
    "Africa"              :    0.483539802,
    "Latin America"       :    0.3326726631,
    "Southeast Asia"      :    0.7552733099,
    "Eastern Europe"      :    0.2212560354,
    "Korea"               :    0.9982557615,
    "India"               :    0.672427639
}

def proc_wdlivestock(infilename, outfilename, rgnTotalFilename):
    """Read and process a table of GCAM livestock water demands.

    We start with a table that looks like so: 
           scenario, region, input, sector, 1990, 2005, 2010, ... , units

    Region and sector are our keys, the year columns are our data, and
    scenario, input, and units are junk
    
    We want a series of tables, one for each animal type, with the form:
           region, 1990, 2005, ... , 2095

    The animal types are buffalo, cattle, goat, sheep, poultry, and
    pig.  Unfortunately, the sectors don't map neatly onto the
    animals.  The sectors are: Beef, Dairy, Pork, Poultry, and
    SheepGoat.  Beef and dairy are summed together and apportioned
    between buffalo and cattle.  SheepGoat are apportioned between
    sheep and goats, and Poultry and Pork map onto poultry and pig,
    respectively.  The coefficients for apportioning the Beef/Dairy
    and SheepGoat sectors are given by region in the tables in
    waterdisag.py.  These are determined from base year data and are
    assumed to be fixed over time.

    """
    wdliv_table = {}
    with open(infilename,"r") as infile:
        ## First read all of the lines in the file
        ## Start by discarding header lines
        infile.readline()
        infile.readline()

        for line in infile:
            line = util.rm_trailing_comma(util.scenariofix(line))
            fields = line.split(',',4) # leave the yearly data in one big string for a moment
            region = fields[1]
            sector = fields[3]
            
            # now split yearly data into fields and convert to numeric
            data = fields[4].split(',') 
            data.pop()          # discard the units field
            
            wdliv_table[(region,sector)] = map(lambda x: float(x), data) # add to master table 
    ## end of file read

    buffalo = {}
    cattle  = {}
    sheep   = {}
    goat    = {}
    poultry = {}
    pig     = {}
    total_wd= {}
    ## loop over regions and compute the withdrawals for each livestock type
    for region in regions_ordered:
        total_bovine    = map(lambda x,y: x+y, wdliv_table[(region,"Beef")], wdliv_table[(region,"Dairy")])
        bfac            = bfracFAO2005[region]
        buffalo[region] = map(lambda x: bfac*x, total_bovine)
        cattle[region]  = map(lambda x: (1-bfac)*x, total_bovine)

        gfac          = gfracFAO2005[region]
        sheepgoat     = wdliv_table[(region,"SheepGoat")]
        goat[region]  = map(lambda x: gfac*x, sheepgoat)
        sheep[region] = map(lambda x: (1-gfac)*x, sheepgoat)

        poultry[region] = wdliv_table[(region,"Poultry")]
        pig[region]     = wdliv_table[(region,"Pork")]

        total_wd[region] = map(lambda tb,sh,pt,pg: tb+sh+pt+pg, total_bovine, sheepgoat, poultry[region], pig[region])
    ## end of loop over regions

    ## write out each table.  the order is:
    ##   buffalo, cattle, goat, sheep, poultry, pig
    with open(outfilename,"w") as outfile:
        for table in [buffalo, cattle, goat, sheep, poultry, pig]:
            wtbl_numeric(outfile, table)

    ## write the total water usage in another file
    wtbl_numeric(rgnTotalFilename, total_wd)
    ## end of write-out

    ## return the master table, in case we want to do something else with it
    return wdliv_table

def wtbl_numeric(outfilein, table):
    """write a table of numeric values by region to a file as CSV data.

    arguments: 
        outfilein - Either a file handle for an open file, or a
                    string.  If a file handle is passed, then write
                    the data and leave the file open.  If a string is
                    passed, open the file under that name, write the
                    data, and close the file.

            table - Data to write.  This data must be numeric, not
                    string data.  The table should be organized by
                    region.  Thus, table[region] should be a list of
                    numeric values.
    
    to an open file.  If given a file handle, write the data and do not
    close the file.  If given a string, open the file, write the data,
    and close

    """
    if hasattr(outfilein,"write"):
        outfile = outfilein
        closeit = False
    else:
        outfile = open(outfilein,"w")
        closeit = True

    try:
        for region in regions_ordered:
            outfile.write(','.join(map(lambda x: str(x), table[region])))
            outfile.write('\n') 
    finally:
        if closeit:
            outfile.close()
## end of wtbl_numeric

### Functions and tables for processing land allocation.

## list of crops.  (crop_table.index(crop)+1) gives the ID number of the crop
## (the same is true of the region table, btw)
## ((conversely, ID-1 is the index of the crop in the list))
croplist = ["Corn", "biomass", "FiberCrop", "MiscCrop", "OilCrop", "OtherGrain",
            "Rice", "Root_Tuber", "SugarCrop", "Wheat", "FodderHerb", "FodderGrass",
            "PalmFruit"]

## some crops are to be treated as generic "biomass"
biomasslist = ["eucalyptus", "Jatropha", "miscanthus", "willow", "biomassOil"]

def proc_irr_share(infilename, outfile):
    """Read the irrigation share table.  

    The table format is:
      region(#), aez, crop(#), 1990, 2005, 2010, ..., 2095, region(text), crop(text)

    The table will be initialized with zeros for all known crops and
    aezs, so any combination not contained in the table will default
    to 0.

    arguments:
       infilename - name of input file
          outfile - name of output file

    """
    ## initialize table with zeros.  Any combination not contained in
    ## the table will default to 0.
    print 'irr share infile: %s' % infilename
    irr_share = {}
    for region in regions_ordered:
        for crop in croplist:
            for aez in range(1,19):
                irr_share[(region,crop,aez)] = [0] * 20
    years = range(2005,2100,5)
    years.insert(0,1990)

    ## irrigation share is a fixed input, so the filename never changes.
    with open(infilename,"r") as infile:
        ## skip 3 header lines
        infile.readline()
        infile.readline()
        infile.readline()

        for line in infile:
            line   = util.rm_trailing_comma(util.chomp(line))
            fields = line.split(',')
            crop   = fields.pop()
            region = fields.pop()
            aez    = int(fields[1])

            irr_share[(region,crop,aez)] = fields # includes region, aez, crop at the beginning
                                                  # these columns are expected by the matlab code
                                                  
    ## end of file read

    ## We need to output this data, as well as using it for other calculations
    with open(outfile,"w") as outfile:
        for region in regions_ordered:
            for aez in range(1,19):
                for crop in croplist:
                    outfile.write(','.join(map(lambda x: str(x), irr_share[(region,crop,aez)])))
                    outfile.write('\n')
            
## end of irrigation share reader

lasplit = re.compile(r'([a-zA-Z_]+)AEZ([0-9]+)')
def proc_ag_area(infilename, outfilename):
    """read in the agricultural area data, reformat, and write out

    The file format is:
      scenario, region, land-allocation (==crop+aez), 1990, 2005, 2010, ..., 2095, units
    The output we want is:
      region-number, aez-number, crop-number, 1990, 2005, 2010, ..., 2095

    The arguments are the input file name and output file name,
    respectively.

    """
    with open(outfilename,"w") as outfile:
        with open(infilename,"r") as infile:
            ## 2 header lines to discard
            infile.readline()        
            infile.readline()

            for line in infile:
                line = util.rm_trailing_comma(util.scenariofix(line))
                #print line
                fields = line.split(',')
                rgntxt = fields[1]
                latxt  = fields[2]
                data   = fields[3:-1]

                rgnno   = regions_ordered.index(rgntxt)+1
                lamatch = lasplit.match(latxt)
                croptxt = lamatch.group(1)
                aezno   = int(lamatch.group(2))

                try:
                    cropno = croplist.index(croptxt) + 1
                except ValueError as e:
                    if croptxt in biomasslist:
                        cropno = croplist.index("biomass") + 1
                    else:
                        raise

                ## prepend the region, aez, and crop numbers to the data
                data.insert(0,cropno)
                data.insert(0,aezno)
                data.insert(0,rgnno)

                #print data
                ## data go out in the same order they came in; we
                ## don't sort by region.
                outfile.write(','.join(map(str,data)))
                outfile.write('\n')
## done with proc_ag_area

def proc_ag_vol(infilename, outfilename):
    """Read in volume of water used by agriculture, reformat, and write out.  

    This function is similar to the previous one, but just different
    enough that we can't reuse the same function.  The input format
    is: 
       scenario, region (text), crop (text), input, sector (crop+AEZ), 1990, 2005, 2010, ..., 2095, units
    The output format is:
      region-number, aez-number, crop-number, 1990, 2005, 2010, ..., 2095

    The arguments are the input and output filenames.

    """
    with open(outfilename,"w") as outfile:
        with open(infilename,"r") as infile:
            ## 2 header lines to discard
            infile.readline()
            infile.readline()

            for line in infile:
                line    = util.rm_trailing_comma(util.scenariofix(line))
                fields  = line.split(',')
                rgntxt  = fields[1]
                croptxt = fields[2] 
                sector  = fields[4]
                data    = fields[5:-1]

                rgnno    = regions_ordered.index(rgntxt) + 1
                secmatch = lasplit.match(sector)
                aezno    = int(secmatch.group(2))
                try:
                    cropno   = croplist.index(croptxt) + 1
                except ValueError as e:
                    if croptxt in biomasslist:
                        cropno = croplist.index('biomass')
                    else:
                        raise

                ## prepend the region, aez, and crop numbers to the data
                data.insert(0, cropno)
                data.insert(0, aezno)
                data.insert(0, rgnno)

                outfile.write(','.join(map(str,data)))
                outfile.write('\n')
## end of proc_ag_vol
