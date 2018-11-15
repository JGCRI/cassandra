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

        self.d1 = DummyComponent(capability_table, 'Alice')
        self.d2 = DummyComponent(capability_table, 'Bob')
        self.d3 = DummyComponent(capability_table, 'Carol')

        self.component_list = [self.d1, self.d2, self.d3]

    def testDelay(self):
        """Test that the time delay is working as expected."""
        self.d1.addparam('capability_reqs', ['Carol'])
        self.d1.addparam('request_delays', [1000])
        self.d1.addparam('finish_delay', 1000)

        self.d2.addparam('capability_reqs', ['Carol'])
        self.d2.addparam('request_delays', [0])
        self.d2.addparam('finish_delay', 1000)

        self.d3.addparam('capability_reqs', [])
        self.d3.addparam('request_delays', [])
        self.d3.addparam('finish_delay', 1000)

        self.runComponents()
        self.confirmSuccess()

        # The last time in the results' times list is the completion time
        finish_times = [c.results['times'][-1][0] for c in self.component_list]

        # Round to the nearest second for comparison
        finish_seconds = [round(ft) for ft in finish_times]

        self.assertEqual(finish_seconds[0], 2)  # 1s before request + 1s finish
        self.assertEqual(finish_seconds[1], 2)  # 1s wait + 1s finish
        self.assertEqual(finish_seconds[2], 1)  # 1s finish

    def confirmSuccess(self):
        for component in self.component_list:
            self.assertEqual(component.status, 1)

    def runComponents(self):
        threads = []

        for component in self.component_list:
            print(f"running {str(component.__class__)}")
            threads.append(component.run())

        # Wait for all threads to complete before printing end message.
        for thread in threads:
            thread.join()


if __name__ == '__main__':
    unittest.main()
