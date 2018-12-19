#!/usr/bin/env python
"""
Perform a test run of Xanthos and check to make sure it provides
the gridded runoff capability.

Note that this test will not run automatically (test scripts
must be named test_*.py ).
"""

from cassandra.components import DummyComponent, XanthosComponent
import unittest
import numpy as np


class TestXanthos(unittest.TestCase):
    def setUp(self):
        """Defines the XanthosComponent."""
        self.xanthos_root = 'cassandra/test/data/xanthos/'

        capability_table = {}

        self.dummy = DummyComponent(capability_table)
        self.dummy.addparam('name', 'ClimateDataGenerator')
        self.dummy.addparam('finish_delay', '100')

        self.dummy.addcapability('gridded_pr')
        self.dummy.addcapability('gridded_tas')
        self.dummy.addcapability('gridded_pr_coord')
        self.dummy.addcapability('gridded_tas_coord')

        self.Xanthos = XanthosComponent(capability_table)
        self.Xanthos.addparam('config_file', f'{self.xanthos_root}trn_abcd_none.ini')

    def testRun(self):
        """Test that Xanthos runs."""
        self.Xanthos.finalize_parsing()
        self.dummy.finalize_parsing()

        t1 = self.Xanthos.run()
        t2 = self.dummy.run()

        coords_npz = np.load(f'{self.xanthos_root}xanthos_coords.npz')
        coords = coords_npz[coords_npz.files[0]]

        # Load test data (5 years worth)
        gridded_pr_npz = np.load(f'{self.xanthos_root}xanthos_pr.npz')
        gridded_tas_npz = np.load(f'{self.xanthos_root}xanthos_tas.npz')
        gridded_pr = gridded_pr_npz[gridded_pr_npz.files[0]]
        gridded_tas = gridded_tas_npz[gridded_tas_npz.files[0]] + 273.15

        self.dummy.addresults('gridded_pr', [gridded_pr])
        self.dummy.addresults('gridded_tas', [gridded_tas])
        self.dummy.addresults('gridded_pr_coord', coords)
        self.dummy.addresults('gridded_tas_coord', coords)

        t1.join()
        t2.join()

        self.assertEqual(self.Xanthos.status, 1)

        results = self.Xanthos.fetch("gridded_runoff")[0]

        self.assertEqual(results.shape, (67420, 36))


if __name__ == '__main__':
    unittest.main()
