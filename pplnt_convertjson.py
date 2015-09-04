import json

def pplnt_convertjson(json_input):
    '''Takes powerplant geoJSON dictionary, returns a list of (lon, lat, val) tuples where lon is longitude,
    lat is latitude, and val is water usage. json_input is powerplant geoJSON dictionary object.'''

    #Get list of plants
    plantlist = json_input["features"]

    #Create and append tuples of lon, lat, and water usage
    output = [(plant["geometry"]["coordinates"][0], plant["geometry"]["coordinates"][1],
               plant["properties"]["water-usage"]) for plant in plantlist]
    
    return(output)


