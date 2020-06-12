#!/usr/bin/env python
"""Test for the Tgav stub component."""

import os
import unittest
import pkg_resources

from cassandra.components import TgavStubComponent

# if necessary, set the path to your R_HOME environment variable
# os.environ['R_HOME'] = '/Library/Frameworks/R.framework/Resources'


class TestTgavStubComponent(unittest.TestCase):

    def setUp(self):
        """Set up a HectorStub component for testing."""

        capability_table = {}

        # configuration
        self.rds_file = pkg_resources.resource_filename('cassandra', 'test/data/fldgen-IPSL-CM5A-LR_test.rds')
        self.climate_var_name = 'tasAdjust'
        self.scenario = 'rcp26'
        self.units = 'Kelvin'
        self.start_year = 1861
        self.through_year = 2099

        # expected output
        self.meta_dict = {'rds_file': self.rds_file,
                          'scenario': self.scenario,
                          'climate_var_name': self.climate_var_name,
                          'source_climate_data': './training-data/tasAdjust_annual_IPSL-CM5A-LR_rcp26_18610101-20991231.nc',
                          'units': self.units,
                          'count': 239,
                          'mean' : 286.8046116940164,
                          'median': 286.30762280534697,
                          'min': 284.78008340211915,
                          'max': 288.6382439866686,
                          'std': 1.182328423261446,
                          'na_count': 0,
                          'null_count': 0,
                          'all_finite': True}

        # instantiate class
        self.stub = TgavStubComponent(capability_table)

        # build parameterization
        self.stub.addparam('rds_file', self.rds_file)
        self.stub.addparam('climate_var_name', self.climate_var_name)
        self.stub.addparam('scenario', self.scenario)
        self.stub.addparam('units', self.units)
        self.stub.addparam('start_year', self.start_year)
        self.stub.addparam('through_year', self.through_year)
        self.stub.finalize_parsing()

        # read in RDS file to dictionary
        self.rds_dict = self.stub.rds_to_dict()

        # generate target file dictionary
        self.target_file_dict = self.stub.build_file_dict(self.rds_dict)

        # generate year list
        self.year_list = self.stub.build_year_list()

        # generate tgav list
        self.tgav_list = self.stub.build_data_list(self.rds_dict, self.target_file_dict, self.year_list)

        # generate tgav dataframe
        self.tgav_df = self.stub.build_dataframe(self.year_list, self.tgav_list)

    def test_log_raise_exception(self):
        """Ensure correct exception is raised."""

        with self.assertRaises(ValueError):
            self.stub.log_raise_exception(ValueError, 'value_error', log_msg=False)

    def test_validate_int(self):
        """Expect correct exception and type return."""

        with self.assertRaises(ValueError):
            self.stub.validate_int('fail')

        val = self.stub.validate_int('1984')
        self.assertTrue(type(val), int)
        self.assertEqual(val, 1984)

    def test_validate_year(self):
        """Expect correct exception."""

        # check min bounds error
        with self.assertRaises(ValueError):
            self.stub.validate_year(-1)

        # check max bounds error
        with self.assertRaises(ValueError):
            self.stub.validate_year(999999)

    def test_validate_file_exist(self):
        """Expect correct exception."""

        with self.assertRaises(FileNotFoundError):
            self.stub.validate_file_exist('/not/a/file.txt')

        fcheck = self.stub.validate_file_exist(self.rds_file)
        self.assertEqual(fcheck, self.rds_file)

    def test_rds_to_dict(self):
        """Check output dict for data."""

        # check for keys
        self.assertTrue('tgav' in self.rds_dict)
        self.assertTrue('infiles' in self.rds_dict)

        # check for data
        self.assertEqual(len(self.rds_dict['tgav']), 956)
        self.assertEqual(len(self.rds_dict['infiles']), 8)

    def test_build_file_dict(self):
        """Ensure correct exception and content."""

        # check for missing infiles key
        with self.assertRaises(KeyError):
            self.stub.build_file_dict({})

        # check for no matching data
        with self.assertRaises(ValueError):
            self.stub.build_file_dict({'infiles': []})

        # check for too many matching files
        with self.assertRaises(ValueError):
            self.stub.build_file_dict({'infiles': ['a', 'a']})

        # valid outputs
        self.assertEqual(self.target_file_dict['files'][0], self.meta_dict['source_climate_data'])
        self.assertEqual(self.target_file_dict['file_index'][0], 0)

    def test_build_year_list(self):
        """Ensure year list returns correct number of years."""

        # check type
        self.assertTrue(type(self.year_list), list)

        # check first and last year
        self.assertEqual(self.year_list[0], self.start_year)
        self.assertEqual(self.year_list[-1], self.through_year)

        # check the number of years
        self.assertEqual(self.through_year - self.start_year + 1, len(self.year_list))

    def test_build_data_list(self):
        """Check expected data outcome."""

        self.assertTrue(type(self.tgav_list), list)

        # check number of values for a single scenario and variable
        self.assertEqual(len(self.tgav_list), 239)

    def test_build_dataframe(self):
        """Confirm fields and shape."""

        # check data frame shape
        self.assertEqual(self.tgav_df.shape, (239, 5))

        # check column names
        self.assertEqual(list(self.tgav_df.columns), ['year', 'scenario', 'variable', 'value', 'units'])

    def test_tgav_metadata(self):
        """Test expected output."""

        meta_dict = self.stub.tgav_metadata(self.tgav_df, self.target_file_dict)

        # check like keys
        self.assertEqual(meta_dict.keys(), self.meta_dict.keys())

        # check value equality
        for k in meta_dict.keys():
            self.assertEqual(meta_dict[k], self.meta_dict[k])

    def test_run_component(self):
        """Test expected output."""

        rval = self.stub.run_component()

        # test run success
        self.assertEqual(rval, 0)


if __name__ == '__main__':
    unittest.main()
