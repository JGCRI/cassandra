#!/usr/bin/env python
"""
Test that components block when they are supposed to and get released when they
are supposed to.
"""

from cassandra.components import *
import unittest


class TestBlocking(unittest.TestCase):
    def setUp(self):
        """Defines the DummyComponents that interact with each other."""
        capability_table = {}

        self.d1 = DummyComponent(capability_table)
        self.d1.addparam('name', 'Alice')
        self.d2 = DummyComponent(capability_table)
        self.d2.addparam('name', 'Bob')
        self.d3 = DummyComponent(capability_table)
        self.d3.addparam('name', 'Carol')

        self.component_list = [self.d1, self.d2, self.d3]
        self.names = [c.params['name'] for c in self.component_list]

    def testDelay(self):
        """Test that the time delay is working as expected."""
        self.d1.addparam('capability_reqs', 'Carol')
        self.d1.addparam('request_delays', '1000')
        self.d1.addparam('finish_delay', '1000')

        self.d2.addparam('capability_reqs', 'Carol')
        self.d2.addparam('request_delays', '0')
        self.d2.addparam('finish_delay', '1000')

        self.d3.addparam('capability_reqs', '')
        self.d3.addparam('request_delays', '')
        self.d3.addparam('finish_delay', '1000')

        self.runComponents()
        self.confirmSuccess()

        # The last time in the results' times list is the completion time
        finish_times = [c.report_test_results()[-1][0] for c in self.component_list]

        # Round to the nearest second for comparison
        finish_seconds = [round(ft) for ft in finish_times]

        self.assertEqual(finish_seconds[0], 2)  # 1s before request + 1s finish
        self.assertEqual(finish_seconds[1], 2)  # 1s wait + 1s finish
        self.assertEqual(finish_seconds[2], 1)  # 1s finish

    def testBlocking(self):
        """Test that a component is blocked while waiting for another."""
        finish_delay = 100

        # Make each component depend on all of the components in front of it
        for i, c in enumerate(self.component_list):
            deps = self.names[i+1:]
            c.addparam('capability_reqs', deps)
            c.addparam('request_delays', ['0'] * len(deps))
            c.addparam('finish_delay', finish_delay)

        self.runComponents()
        self.confirmSuccess()

        # Calculate the ms it took to ran the first component
        d1_time = self.d1.report_test_results()[-1][0]
        d1_ms = round(d1_time, 1) * 1000

        # Should have had to wait for each component to finish; the only delay
        # in this test is the finish delay
        self.assertEqual(d1_ms, finish_delay * len(self.component_list))

    def confirmSuccess(self):
        """Ensure each component finished successfully."""
        for component in self.component_list:
            self.assertEqual(component.status, 1)

    def runComponents(self):
        """Run each component."""
        threads = []

        for comp in self.component_list:
            comp.finalize_parsing() 
        
        for component in self.component_list:
            threads.append(component.run())

        # Wait for all threads to complete before printing end message.
        for thread in threads:
            thread.join()


if __name__ == '__main__':
    unittest.main()
