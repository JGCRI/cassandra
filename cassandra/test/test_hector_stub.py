#!/usr/bin/env python
"""
Test that the hector stub component loads its data correctly.

"""

from cassandra.components import HectorStubComponent
import unittest


class TestHectorStub(unittest.TestCase):
    def setUp(self):
        """Set up a HectorStub component for testing."""

        capability_table = {}

        self.hstub = HectorStubComponent(capability_table)
        self.hstub.addparam('scenarios', ['rcp26', 'rcp45', 'rcp60', 'rcp85'])
        self.hstub.finalize_parsing()
        self.hstub.run_component()

    def testTgav(self):
        """Test that Tgav data was read correctly."""

        tgav = self.hstub.results['Tgav']
        self.assertEqual(tgav.shape, (2220, 5))
        self.assertEqual(list(tgav.columns),
                         ['year', 'scenario', 'variable', 'value', 'units'])

        tgav2100 = tgav[tgav['year'] == 2100]
        self.assertEqual(tgav2100.shape, (4, 5))
        self.assertEqual(list(tgav2100['value']),
                         [1.541, 2.510, 3.114, 4.604])

    def testco2(self):
        """Test that atmospheric CO2 data was read correctly."""

        co2 = self.hstub.results['atm-co2']
        self.assertEqual(co2.shape, (2220, 5))
        self.assertEqual(list(co2.columns),
                         ['year', 'scenario', 'variable', 'value', 'units'])

        co2_2100 = co2[co2['year'] == 2100]
        self.assertEqual(co2_2100.shape, (4, 5))
        self.assertEqual(list(co2_2100['value']),
                         [392.2, 514.3, 659.6, 905.3])

    def testFtot(self):
        """Test that radiative forcing data was read correctly."""

        ftot = self.hstub.results['Ftot']
        self.assertEqual(ftot.shape, (2204, 5))  # For some reason, Ftot starts a few years later than the other vars.
        self.assertEqual(list(ftot.columns),
                         ['year', 'scenario', 'variable', 'value', 'units'])

        ftot2100 = ftot[ftot['year'] == 2100]
        self.assertEqual(ftot2100.shape, (4, 5))
        self.assertEqual(list(ftot2100['value']),
                         [2.294, 4.101, 5.477, 8.412])


if __name__ == '__main__':
    unittest.main()
