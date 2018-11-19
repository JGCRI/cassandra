#!/usr/bin/env python
"""
Test that components add capabilities as expected, and errors when adding a
duplicate or fetching a non-existing capability.
"""

from cassandra.components import DummyComponent, CapabilityNotFound
import unittest


class TestCapabilities(unittest.TestCase):
    def setUp(self):
        """Defines the DummyComponents that interact with each other."""
        capability_table = {}

        self.d1 = DummyComponent(capability_table)
        self.d1.addparam('name','Alice')
        self.d1.addparam('finish_delay', '1000')
        self.d2 = DummyComponent(capability_table)
        self.d2.addparam('name', 'Bob')
        self.d2.addparam('finish_delay', '1000')

        self.d1.finalize_parsing()
        self.d2.finalize_parsing()

    def testAddCap(self):
        """Test that adding a capability puts it in the capability table"""
        # The name of the component should be in there from initialization
        self.assertIn('Alice', self.d1.cap_tbl)
        self.d1.addcapability('NewCapability')
        self.assertIn('NewCapability', self.d1.cap_tbl)

    def testAddCapErr(self):
        """Test that adding a duplicate capability errors"""
        self.assertRaises(RuntimeError, self.d1.addcapability, 'Alice')

    def testGetNonExistErr(self):
        """Test that fetching a non-existing capability errors"""
        self.assertRaises(CapabilityNotFound, self.d1.fetch, 'NotACapability')

    def testAddOtherComponentCapErr(self):
        """Test to ensure we can't add results to another component's capability."""
        data = 'ResultsFromAlice'
        self.d1.addresults('Alice', data)
        self.assertEqual(self.d1.results['Alice'], data)
        self.assertRaises(RuntimeError, self.d1.addresults, 'Bob', data)


if __name__ == '__main__':
    unittest.main()
