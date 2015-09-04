import json

def convert_capacity(plant):
    """Converts powerplant capacity string from geoJSON data to float and returns capacity.
    plant is a powerplant object in geoJSON format."""
    capacity = plant["properties"]["capacity"]
    capacity = float(capacity.replace("MWe",""))                
    return(capacity)
    
def getWaterUsage(file, dict1):
    """Takes powerplant geoJSON raw data and returns python dictionary of data with water usage included.
    file is file handle for json input, dict1 is dictionary of water usage conversion factors."""

    #Load original geoJSON file. This is the plant dictionary. 
    raw_json = json.load(file) #Note: doesn't matter if file is JSON or GeoJSON
     
    #Multiply plant capacity (MWe) by water usage factor (km^3/MWe) from dictionary to get list of water usage data. 
    #Add this data to plant dictionary. 
    for plant in raw_json["features"]:
        plant["properties"]["water-usage"] = convert_capacity(plant)*dict1[plant["properties"]["fuel"]] 
    
    return(raw_json)   #Returns updated dictionary of plants and features.  

