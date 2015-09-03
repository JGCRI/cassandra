import json
import ast

def getWaterUsage(file, dictionary):
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
    
    return(json.dumps(output))   #Returns json format string. If using json.loads instead, returns a dictionary. 

        
#Driver
infile = open("toy.json", "r")
#outfile = open("new.json", 'w')
dict1 = {'Coal': 1, 'Gas':2, 'Nuclear':3} #Insert conversion factors here. 

z = getWaterUsage(infile, dict1)
print(json.loads(z))        #Prints correctly, can be written to file. 
#outfile.write(json.loads(z))

#Convert string to dictionary (will lose formatting)
a = json.loads(z)
b = ast.literal_eval(a) #Converts to dictionary.
print(b["type"])        #Now can use dictionary keys.
print(b)                #New format is just a block of text

      
infile.close()
#outfile.close()
