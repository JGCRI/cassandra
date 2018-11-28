#!/usr/bin/env python
"""
Perform a test run of Tethys and check to make sure it provides
spatial and temporal capabilities.

Note that this test will not run automatically (test scripts
must be named test_*.py ).
"""

from cassandra.components import *
import unittest


class TestTethys(unittest.TestCase):
    def setUp(self):
        """Defines the TethysComponent."""
        capability_table = {}

        self.tethys = TethysComponent(capability_table)
        self.tethys.addparam("config_file", "/Users/brau074/Documents/tethys/example/config.ini")

    def testRun(self):
        """Test that Tethys runs."""
        self.tethys.finalize_parsing()
        t = self.tethys.run()
        t.join()

        self.assertEqual(self.tethys.status, 1)
        self.assertEqual(self.tethys.fetch("gridded_water_demand_nonag").shape, (259200, 5))
        self.assertEqual(self.tethys.fetch("gridded_monthly_water_demand_dom").shape, (67420, 72))


if __name__ == '__main__':
    unittest.main()
