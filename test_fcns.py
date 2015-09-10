#Test pplnt water and pplnt_convertjson
import pplntwater
import pplnt_convertjson
import pplnt_writecsv
import json

###Driver
infile = open('toy.json', 'r')
dict1 = {'Coal': 1, 'Gas':2, 'Nuclear':3}

#Get python dictionary with water usage factors from json file
x = pplntwater.getWaterUsage(infile, dict1)
print(x)

#List of (lon, lat, val) tuples
z = pplnt_convertjson.pplnt_convertjson(x)
print(z)

a = pplnt_writecsv.pplnt_writecsv(z)

infile.close()
