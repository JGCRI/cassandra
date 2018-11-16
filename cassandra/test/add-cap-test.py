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

        self.d1 = DummyComponent(capability_table, 'Alice')

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


if __name__ == '__main__':
    unittest.main()
