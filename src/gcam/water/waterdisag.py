#!/bin/env python
"""Functions supporting the water demand disaggregation module."""


from gcam import util
import re
from sys import stderr,stdout

## Canonical ordering of the regions for outputs
regions_ordered = []
## Reverse lookup table for region ordering
rgnid = {}

## Year-2000 population benchmarks
gis2000 = {}

## table giving the fraction of beef that is buffalo (vs. cattle), by region
bfracFAO2005 = {}

## table giving the fraction of SheepGoat that is goat (vs. sheep), by region
gfracFAO2005 = {}



def init_rgn_tables(rgnconfig):
    """Read the region-specific data tables in the rgnconfig directory.

    These tables are used internally for the calculations in this
    (python) module.

    """

    global gis2000
    global bfracFAO2005
    global gfracFAO2005
    global regions_ordered
    
    (rgnid, regions_ordered) = util.rd_rgn_table('%s/RgnNames.txt'%rgnconfig)
    (gis2000, _) = util.rd_rgn_table('%s/gis2000.csv'%rgnconfig)
    (bfracFAO2005, _) = util.rd_rgn_table('%s/bfracFAO2005.csv'%rgnconfig)
    (gfracFAO2005, _) = util.rd_rgn_table('%s/gfracFAO2005.csv'%rgnconfig) 
##end    
    
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
        ## skip comment line
        file.readline()

        ## check for 2100 column (which must be dropped)
        fields = util.rm_trailing_comma(file.readline()).split(',')
        if fields[-2]=='2100':
            rm2100 = True
            stdout.write('[rd_gcam_table]:  dropping 2100 column from file %s\n'%filename)
        else:
            rm2100 = False

        for line in file:
            line = util.rm_trailing_comma(line)
            linefix = util.scenariofix(line)
            ## split off the first two columns
            linesplit = linefix.split(',',2+njunk)

            region = linesplit[1] 
            # trim the final (units) column.  Note that this will also
            # trim off the newline.
            data = (linesplit[-1].rpartition(','))[0]
            # if necessary, trim the 2100 column too.
            if rm2100:
                data = (data.rpartition(','))[0]
                
            table[region] = data

    return table


            
## reorder the table output and write it to a file.  
def table_output_ordered(filename, table, incl_region=False, ordering=None):
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

    if ordering is None:
        ordering = regions_ordered

    try: 
        with open(filename,"w") as file:
            for region in ordering:
                if incl_region:
                    file.write("%s,%s\n" % (region, table[region]))
                else:
                    file.write("%s\n" % table[region])
    except KeyError as e:
        stderr.write('[table_output_ordered]: Region not found: %s\n' % e)
        stderr.write('[table_output_ordered]: input table keys:\n\t%s\n' % table.keys())
        raise e
    
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

    The input population table has the following format:
    rows -
          comment line
          header line
          data1
          ...
          datan
    columns (entries followed by a '?' are present in some versions, but not in others) -
          scenario, region, 1990, 2005, 2010, 2015, ... , 2095, 2100(?), units

    arguments:
          infile - Input data file.
     outfile_fac - Output file for the "pop_fac" table
     outfile_tot - Output file for the "pop_tot" table 
    outfile_demo - Output file for the reordered version of the
                   "pop_tot" table. (TODO: This output is probably
                   obsolete.)

    """

    poptbl = rd_gcam_table(infile,0)
    pop_fac = {}
    pop_tot = {}

    for region in poptbl.keys():
        stderr.write('[proc_pop]: processing region: %s\n' % region)
        popvals   = poptbl[region].split(',')
        benchmark = gis2000[region]
        fpop      = map(lambda x: str(1000*float(x)/benchmark), popvals)
        totpop    = map(lambda x: str(1000*float(x)), popvals)

        pop_fac[region] = ','.join(fpop)
        pop_tot[region] = ','.join(totpop)

    #stderr.write('[proc_pop]: popfac:\n%s\n'%str(pop_fac))
    #stderr.write('[proc_pop]: poptot:\n%s\n'%str(pop_tot))
        
    table_output_ordered(outfile_fac, pop_fac)
    table_output_ordered(outfile_tot, pop_tot)
    table_output_ordered(outfile_demo, pop_tot) # demo output now uses the normal ordering.

    return (pop_fac, pop_tot) 



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
        ## Start by discarding comment line
        infile.readline()

        ## read header line to determine whether we have to discard a 2100 column
        fields = util.rm_trailing_comma(infile.readline()).split(',')
        stdout.write('[proc_wdlivestock]: header:  %s\n'%str(fields))
        if fields[-2] == '2100':
            rm2100 = True
            nyear = len(fields) - 6
            stdout.write('[proc_wdlivestock]: dropping 2100 column from file %s.\n' % infilename)
        else:
            rm2100 = False
            nyear = len(fields) - 5
        stdout.write('[proc_wdlivestock]: nyear= %d\n'%nyear)

        for line in infile:
            line = util.rm_trailing_comma(util.scenariofix(line))
            fields = line.split(',',4) # leave the yearly data in one big string for a moment
            region = fields[1]
            sector = fields[3]
            
            # now split yearly data into fields and convert to numeric
            data = fields[4].split(',') 
            data.pop()          # discard the units field
            if rm2100:
                data.pop()
            
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
        try:
            wdbeef = wdliv_table[(region,'Beef')]
        except KeyError: 
            wdbeef = [0]*nyear  # region/livestock combinations not appearing are zero
        try:
            wddairy = wdliv_table[(region,'Dairy')]
        except KeyError:
            wddairy = [0]*nyear
        total_bovine    = map(lambda x,y: x+y, wdbeef, wddairy)
        bfac            = bfracFAO2005[region]
        buffalo[region] = map(lambda x: bfac*x, total_bovine)
        cattle[region]  = map(lambda x: (1-bfac)*x, total_bovine)

        try:
            gfac          = gfracFAO2005[region]
            sheepgoat     = wdliv_table[(region,"SheepGoat")]
            goat[region]  = map(lambda x: gfac*x, sheepgoat)
            sheep[region] = map(lambda x: (1-gfac)*x, sheepgoat)
        except KeyError:
            goat[region]  = [0]*nyear
            sheep[region] = [0]*nyear

        try:
            poultry[region] = wdliv_table[(region,"Poultry")]
        except KeyError:
            poultry[region] = [0]*nyear

        try:
            pig[region]     = wdliv_table[(region,"Pork")]
        except KeyError:
            pig[region]     = [0]*nyear

        try:
            total_wd[region] = map(lambda tb,sh,pt,pg: tb+sh+pt+pg, total_bovine, sheepgoat, poultry[region], pig[region])
        except TypeError:
            stderr.write('[proc_wdlivestock]: bad table data for region = %s.\n' % region)
            stderr.write('\ttotal_bovine: %s\n' % str(total_bovine))
            stderr.write('\tsheepgoat:    %s\n' % str(sheepgoat))
            stderr.write('\tpoultry:      %s\n' % str(poultry[region]))
            stderr.write('\tpig:          %s\n' % str(pig[region]))
            raise
        
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

lasplit = re.compile(r'([a-zA-Z_ ]+)AEZ([0-9]+)(IRR|RFD)?')
def proc_ag_area(infilename, outfilename,drop2100=True):
    """read in the agricultural area data, reformat, and write out

    The output we want is:
      region-number, aez-number, crop-number, 1990, 2005, 2010, ..., 2095

    Furthermore, if GCAM produced separate totals for irrigated and
    rain-fed crops, we want to include only the irrigated.  So, we
    skip any allocations for the rain-fed versions of a crop.  Earlier
    versions of GCAM did not make the distinction, so if we are
    running on one of those output files we have to correct the total
    planted area with a precalculated irrigation fraction in a later
    step.

    Arguments:
      infilename  - Name of the input file
      outfilename - Name of the output file
      drop2100    - Flag:  True  = drop the 2100 data column (if present);
                           False = keep 2100 (if present)

    return value: Flag indicating whether the GCAM run produced an
                   endogeneous allocation between irrigated and rain-fed
                   crops.

    """

    ## Flag indicating whether we are using GCAM's irrigation.  We
    ## won't know for sure until we read the first line of data.
    ## Start by assuming it is.
    gcam_irr = True
    
    ag_area = read_gcam_ag_area(infilename, drop2100)

    with open(outfilename,"w") as outfile: 
        for (key,data) in ag_area.items():
            (rgnno, aezno, cropno, irr) = key
            if irr == 'TOT':
                ## indicate that GCAM is providing total area
                gcam_irr = False
            elif irr == 'RFD':
                ## do not include rain-fed crops in the result
                continue

            ## prepend the region, aez, and crop numbers to the data
            data.insert(0,cropno)
            data.insert(0,aezno)
            data.insert(0,rgnno)

            ## No requirement to sort this table by region, so just
            ## output in the order used by the dictionary
            outfile.write(','.join(map(str,data)))
            outfile.write('\n')

    stdout.write('[proc_ag_area]: gcam_irr = %s\n' % gcam_irr)
    return gcam_irr
## done with proc_ag_area


def read_gcam_ag_area(infilename, drop2100=True):
    """Read in the agricultural area table produced by gcam and return as a table.

    The file row format is:
      comment line
      header line
      data

    The file column format is ('?' indicates a column that is not always present):
      scenario, region, land-allocation (==crop+aez), 1990, 2005, 2010, ..., 2095, 2100 (?), units

    We will store the table in a dictionary indexed by (region-number,
    aez-number, crop-number, irrigated).  The data will be a list of
    output land area values for each region,aez,crop,irrigation
    combination.  The irrigation values can take on one of three
    values:
       IRR - irrigated
       RFD - rain fed
       TOT - total

    The last of these will only occur for GCAM output data that does
    not distinguish between irrigated and rain fed crops (i.e., older
    versions of GCAM).  In these cases, all irrigation values will be
    TOT, with no IRR or RFD values appearing.  Thus, the presence of a
    TOT irrigation type serves as a reliable indicator of whether or
    not a correction for irrigation fraction is needed.

    Arguments:
      infilename   - input file name
      drop2100     - flag: True = drop the 2100 column from the output.

    Return value: 
      Dictionary containing the table, indexed as indicated above.
      Table values will be numerical, not strings.

    """

    table = {}
    with open(infilename,"r") as infile:
        ## discard comment line
        infile.readline()

        ## Check the header line to see if we have a year 2100.
        ## If so, discard it in all data lines.
        fields = util.rm_trailing_comma(infile.readline()).split(',')
        if fields[-2] == '2100' and drop2100:
            lstfld = -2     # i.e., data = fields[3:-2], dropping the last 2 columns
        else:
            lstfld = -1     # i.e., data = fields[3:-1], dropping only the last column

        for line in infile:
            line = util.rm_trailing_comma(util.scenariofix(line))
            fields    = line.split(',')
            rgntxt    = fields[1]
            latxt     = fields[2]           # land area text
            datastr   = fields[3:lstfld]    # chop off units column and possibly 2100 column.
            data      = map(lambda s:float(s), datastr)

            ## split land area text to get crop, aez, and possibly irrigation status
            lamatch = lasplit.match(latxt)
            croptxt = lamatch.group(1)
            aezno   = int(lamatch.group(2))
            irrstat = lamatch.group(3)
            if irrstat is None:
                irrstat = 'TOT' 

            ## convert region and crop to index numbers
            rgnno   = regions_ordered.index(rgntxt)+1 
            try:
                cropno = croplist.index(croptxt) + 1
            except ValueError as e:
                if croptxt in biomasslist:
                    cropno = croplist.index("biomass") + 1
                else:
                    raise

            table[(rgnno,aezno,cropno,irrstat)] = data

        return table 

def proc_ag_vol(infilename, outfilename):
    """Read in volume of water used by agriculture, reformat, and write out.  

    This function is similar to the previous one, but just different
    enough that we can't reuse the same function.  The input format
    is: 
       scenario, region (text), crop (text), input, sector (crop+AEZ), 1990, 2005, 2010, ..., 2095, units
    The output format is:
      region-number, aez-number, crop-number, 1990, 2005, 2010, ..., 2095

    Arguments:
      infilename  - Name of input file
      outfilename - Name of the output file

    Return value:  None 

    """
    
    with open(outfilename,"w") as outfile:
        with open(infilename,"r") as infile:
            ## 2 header lines to discard
            ## XXX do we need to drop 2100 column here?  We tested without
            ##     and it didn't seem to hurt anything.
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
