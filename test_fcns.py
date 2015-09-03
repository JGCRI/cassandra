#Test pplnt water and pplnt_convertjson
import pplntwater
import pplnt_convertjson
import json
import ast

###Driver
infile = open('toy.json', 'r', encoding = 'utf-8')
dict1 = {'Coal': 1, 'Gas':2, 'Nuclear':3}

#Formatted json string
x = pplntwater.getWaterUsage(infile, dict1)
print(x) 

#Dictionary version of json string
y = ast.literal_eval(x)
print(y["features"][1])

#List of (lon, lat, val) tuples
z = pplnt_convertjson.pplnt_convertjson(x)
print(z)


infile.close()
