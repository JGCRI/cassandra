#!/usr/bin/env python3
"""Convert hector outputstreams to pickled python objects

This program was run on the sample_outputstream*.csv files distributed with
hector to produce the *.dat files in the data directory of this package.

""" 

import pandas as pd
import pickle

for rcp in ['rcp26', 'rcp45', 'rcp60', 'rcp85']:
    infile = f'hector-outputstream-{rcp}.csv'
    outputstream = pd.read_csv(infile, comment='#')
    outfile = open(f'hector-outputstream-{rcp}.dat', 'wb')
    pickle.dump(outputstream, outfile)
    outfile.close()
