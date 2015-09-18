#!/usr/bin/env python
"""Test functions for processing power plant water data.
"""

import sys
## enable this code to run from the gcam/test/ directory
sys.path.append('../..')

import gcam.water
from gcam.water import pplnt
import json

###Driver
infile = open('data/toy.json', 'r')          
dict1 = {'Coal': 1, 'Gas':2, 'Nuclear':3}

#Get python dictionary with water usage factors from json file
x = pplnt.getWaterUsage(infile, dict1)
print(x)

#List of (lon, lat, val) tuples
z = pplnt.pplnt_convertjson(x)
print(z)
print(pplnt.pplnt_grid(z))

a = pplnt.pplnt_writecsv(z,'pplnt-out-test.csv')

infile.close()
