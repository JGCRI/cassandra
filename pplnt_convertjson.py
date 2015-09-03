import json
import ast

def pplnt_convertjson(json_input):
#Takes powerplant geoJSON data, returns a list of (lon, lat, val) tuples.
    #Convert input string to dictionary
    if type(json_input)==str:
        json_mod = ast.literal_eval(json_input)
    elif type(json_input)==dict:
        json_mod = json_input

    #Get list of features
    featurelist = json_mod["features"]

    #Initialize output list
    output = list()

    #Create and append tuples of lat, lon, and water usage
    for i in range(0,len(featurelist)):
        tuple1 = (featurelist[i]["geometry"]["coordinates"][0], featurelist[i]["geometry"]["coordinates"][1],
                  featurelist[i]["properties"]["water-withdrawal"])
        output.append(tuple1)
        
    return(output)


