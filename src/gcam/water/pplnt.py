"""Functions for processing water usage from power plant location data."""

import json
import csv


def pplnt_grid(tuple_list, resolution=[.5, .5], boundary= [-180, -90, 180, 90]):
    """Takes a list of (lon, lat, water_usage) tuples ('tuple_list') and returns dictionary with tuples assigned
    to a grid cell with key (i,j).
    Resolution: (i, j) dimensions of individual grid cells.
    Boundary: (lon0, lat0, lon1, lat1) where(lon0,lat0) are (lon, lat) coordinates of lower left corner of grid
    and (lon1, lat1) are (lon, lat) coordinates of upper right corner of grid."""

    #Initialize grid dictionary
    grid = {}
    
    #Get dimensions of grid cell
    i_dim = resolution[0]  #length of side i
    j_dim = resolution[1]  #length of side j

    #Map (lon,lat) dimensions to (i,j) dimensions
    lon_start = boundary[0] 
    lat_start = boundary[1]
    lon_end = boundary[2]
    lat_end = boundary[3]

    i_start = 0 
    j_start = 0 

    lon_offset = abs(lon_start - i_start)
    lat_offset = abs(lat_start - j_start)

    i_end = int((lon_end + lon_offset)/i_dim) #Note: will truncate end of lon,lat if not evenly divisible by i_dim, j_dim
    j_end = int((lat_end + lat_offset)/j_dim)
    
    #Add (lon, lat, water_usage) tuple to grid using (i, j) key
    for n in range(0, len(tuple_list)):
        #Calculate (i,j) values
        #Ex: lon values of -180 to -179.51 are 0, -179.5 to -179.01 are 1, etc.
        i = int((tuple_list[n][0]+lon_offset)/i_dim)
        j = int((tuple_list[n][1]+lat_offset)/j_dim)

        #Reassign points on upper boundaries to previous grid cell
        if i==i_end:
            i = i_end - 1
        if j==j_end:
            j = j_end-1

        #Only add to grid if within grid boundaries. Include edges. 
        if (i>=i_start and i<i_end) and (j>=j_start and j<j_end):
            if (i,j) in grid:
                grid[(i,j)] += tuple_list[n][-1] # last entry is the value we want
            else:
                grid[(i,j)] = tuple_list[n][-1]
    
    return(grid)


def pplnt_convertjson(json_input):
    '''Takes powerplant geoJSON dictionary, returns a list of (lon, lat, val) tuples where lon is longitude,
    lat is latitude, and val is water usage. json_input is powerplant geoJSON dictionary object.'''

    #Get list of plants
    plantlist = json_input["features"]

    #Create and append tuples of lon, lat, and water usage
    output = [(plant["geometry"]["coordinates"][0], plant["geometry"]["coordinates"][1],
               plant["properties"]["water-usage"]) for plant in plantlist]
    
    return(output)


def pplnt_writecsv(grid_as_dict, filename, comment=None):
    """Takes list of (lon, lat, water-usage) tuples ('tuple_list') and a string indicating the
    name of the output file ('filename') and writes to a 3-column csv ('outfile')."""

    outfile = open(filename, 'w')          # python3 note:  should use newline=''
    if comment is None:
        outfile.write('#power plant data\n')
    else:
        outfile.write('#%s\n'%comment)

    csv_write = csv.writer(outfile, delimiter=',', lineterminator='\n') # lineterminator= not needed in python3 (see comment above)
    csvrows = ((key[0],key[1],value) for key,value in grid_as_dict.iteritems())
    csv_write.writerows(csvrows)

    outfile.close()

def convert_capacity(plant):
    """Converts powerplant capacity string from geoJSON data to float and returns capacity.
    plant is a powerplant object in geoJSON format."""
    capacity = plant["properties"]["capacity"]
    capacity = float(capacity.replace("MWe",""))                
    return(capacity)
    
def getWaterUsage(file, water_conversion_dict):
    """Takes powerplant geoJSON raw data and returns python dictionary of data with water usage included.
    file is file handle for json input, water_conversion_dict is dictionary of water usage conversion factors."""

    #Load original geoJSON file. This is the plant dictionary. 
    raw_json = json.load(file) #Note: doesn't matter if file is JSON or GeoJSON
     
    #Multiply plant capacity (MWe) by water usage factor (km^3/MWe) from dictionary to get list of water usage data. 
    #Add this data to plant dictionary. 
    for plant in raw_json["features"]:
        plant["properties"]["water-usage"] = convert_capacity(plant)*water_conversion_dict[plant["properties"]["fuel"]] 
    
    return(raw_json)   #Returns updated dictionary of plants and features.  

