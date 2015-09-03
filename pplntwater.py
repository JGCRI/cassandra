import json

def getWaterUsage(file, dictionary):
#"""Takes powerplant geoJSON raw data and adds water usage."""
    #Load original geoJSON file
    raw_json = json.load(file) #Note: doesn't matter if file is JSON or GeoJSON
    
    #Get list of features
    featurelist = raw_json["features"]

    #geoJSON Template
    template =  '''{"type": "Feature", "geometry": {"type": "Point", "coordinates": [%.2f, %.2f]},
        "properties": {"name": "%s",
        "country": "%s", "fuel": "%s", "capacity": "%s", "water-withdrawal": %.2f, "units": "km^3"}
        },
    '''
    #End template without comma
    template_end = '''{"type": "Feature", "geometry": {"type": "Point", "coordinates": [%.2f, %.2f]},
        "properties": {"name": "%s",
        "country": "%s", "fuel": "%s", "capacity": "%s", "water-withdrawal": %.2f, "units": "km^3"}
        }
    '''
    
    #Head of geoJSON file
    output = '''{"type": "FeatureCollection", "features":[
    '''
    
    #Get water usage data
    capacities = list()
    water = list()
    
    for i in range(0,len(featurelist)):
        #Take out "MWe" characters from capacity, convert to int
        string1 = featurelist[i]["properties"]["capacity"]
        newstr = string1.replace("MWe", "")
        cap = int(newstr)
        capacities.append(cap)
    
        #Get water usage data from capacity and dictionary 
        usage_factor = dictionary[featurelist[i]["properties"]["fuel"]]     #km^3/MWe
        water_usage = usage_factor*capacities[i]    #km^3
        water.append(water_usage)

        #Write to new geoJSON
        if i != (len(featurelist)-1):
            output += template % (featurelist[i]["geometry"]["coordinates"][0], featurelist[i]["geometry"]["coordinates"][1],
                                  featurelist[i]["properties"]["name"], featurelist[i]["properties"]["country"], featurelist[i]["properties"]["fuel"],
                                  featurelist[i]["properties"]["capacity"], water[i])
        #Last iteration without comma
        elif i== (len(featurelist)-1):
            output += template_end % (featurelist[i]["geometry"]["coordinates"][0], featurelist[i]["geometry"]["coordinates"][1],
                                  featurelist[i]["properties"]["name"], featurelist[i]["properties"]["country"], featurelist[i]["properties"]["fuel"],
                                  featurelist[i]["properties"]["capacity"], water[i])


    
    
    #Add tail of geoJSON
    output += ''']
}'''
    out = json.dumps(output)
    return(json.loads(out))   #Returns json format string. Use ast.literal.eval(out) to convert to dictionary.  

