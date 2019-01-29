#!/usr/bin/env python
"""
Perform a test run of Xanthos.

A DummyComponent is used to imitate a component that would generate
the gridded precipitation and temperature data, such as fldgen.
The inputs are loaded from sample files containing three years of
test data, using the Xanthos configuration file in the data
directory.

The tests check that Xanthos runs correctly and produces a gridded
runoff result. The values are not checked, as this lies in the
domain of the Xanthos package, not cassandra.
"""

from cassandra.components import DummyComponent, XanthosComponent
import unittest
import numpy as np


class TestXanthos(unittest.TestCase):
    def setUp(self):
        """Defines the XanthosComponent."""
        self.xanthos_root = 'cassandra/test/data/xanthos/'

        capability_table = {}

        self.Xanthos = XanthosComponent(capability_table)
        self.Xanthos.addparam('config_file', f'{self.xanthos_root}trn_abcd_none.ini')

        # Create a component that will simulate a gridded climate data generator
        self.dummy = DummyComponent(capability_table)
        self.dummy.addparam('name', 'ClimateDataGenerator')
        self.dummy.addparam('finish_delay', '100')

        self.dummy.addcapability('gridded_pr')
        self.dummy.addcapability('gridded_tas')
        self.dummy.addcapability('gridded_pr_coord')
        self.dummy.addcapability('gridded_tas_coord')

    def testRun(self):
        """Test that Xanthos runs with input from a capability."""
        self.Xanthos.finalize_parsing()
        self.dummy.finalize_parsing()

        t1 = self.Xanthos.run()
        t2 = self.dummy.run()

        self.simulateClimateGen()

        t1.join()
        t2.join()

        self.assertEqual(self.Xanthos.status, 1)

        results = self.Xanthos.fetch("gridded_runoff")[0]

        self.assertEqual(results.shape, (67420, 36))

    def simulateClimateGen(self):
        """Simulate a climate data generating component.

        Rather than actually run fldgen, or some other component that outputs
        gridded climate data, load a simple test dataset. In reality, these
        capabilities would be added by some component's run_component() method.

        """
        # Test coordinates are the Xanthos lat/lon coordinate map
        coords_npz = np.load(f'{self.xanthos_root}xanthos_coords.npz')
        coords = coords_npz[coords_npz.files[0]]

        # Load test data (5 years worth); values are from Xanthos' example
        # input data, but rounded for better compression
        gridded_pr_npz = np.load(f'{self.xanthos_root}xanthos_pr.npz')
        gridded_tas_npz = np.load(f'{self.xanthos_root}xanthos_tas.npz')
        gridded_pr = gridded_pr_npz[gridded_pr_npz.files[0]]
        gridded_tas = gridded_tas_npz[gridded_tas_npz.files[0]] + 273.15

        self.dummy.addresults('gridded_pr', [gridded_pr])
        self.dummy.addresults('gridded_tas', [gridded_tas])
        self.dummy.addresults('gridded_pr_coord', coords)
        self.dummy.addresults('gridded_tas_coord', coords)


if __name__ == '__main__':
    unittest.main()
