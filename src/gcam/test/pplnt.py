#!/usr/bin/env python
"""Test functions for processing power plant water data.
"""

import sys
## enable this code to run from the gcam/test/ directory
sys.path.append('../..')

import gcam.water
from gcam.water import pplntwater
from gcam.water import pplnt_convertjson
from gcam.water import pplnt_writecsv
from gcam.water import pplnt_grid
import json

###Driver
infile = open('data/toy.json', 'r')
dict1 = {'Coal': 1, 'Gas':2, 'Nuclear':3}

#Get python dictionary with water usage factors from json file
x = pplntwater.getWaterUsage(infile, dict1)
print(x)

#List of (lon, lat, val) tuples
z = pplnt_convertjson.pplnt_convertjson(x)
print(z)
print(pplnt_grid.pplnt_grid(z))

a = pplnt_writecsv.pplnt_writecsv(z)

infile.close()
