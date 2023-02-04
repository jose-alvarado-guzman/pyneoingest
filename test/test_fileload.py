# -*- coding: utf-8 -*-
"""Module use for testing the fileload package.

    Classes
    -------
    TestFileloadHelpers
        Use for testing the helper functions.
    TestFileloadYaml
        Use to test the function that loads a YAML file and return a dictionary.
"""
import os
import unittest
from .context import fileload
from . import test_dir

class TestFileloadHelpers(unittest.TestCase):
    """Class use for testing the functions in the the helper module.
        Method
        ------
        test_lower_dict_keys()
            Test the function that converts dictionaries keys to lower case.
        test_check_required_keys()
            Test the function that check that a YAML file contains the provided
            keys.
    """

    def test_lower_dict_keys(self):
        """Test the convertion of dictionary keys to lower case."""
        test_dict = {'ID':1,'Name':'John Smith'}
        solution = {'id':1,'name':'John Smith'}
        result = fileload.lower_dict_keys(test_dict)
        self.assertEqual(result, solution)

    def test_check_required_keys(self):
        """Test that a YAML file contains the provided keys.

            It contains the following 2 different scenarios:
            1. The file contains all the required keys.
            2. The file is missing seveal keys.
        """
        require_list = ['neo4j_user','neo4j_password','uri']
        provided_list1 = ['neo4j_user','neo4j_password','uri','database']
        solution1 = []
        provided_list2 = ['neo4j_user','cypher','files','pre_ingest']
        result1 = fileload.check_required_keys(require_list, provided_list1)
        result2 = fileload.check_required_keys(require_list, provided_list2)
        solution2 = ['neo4j_password','uri']
        self.assertEqual(solution1,result1)
        self.assertEqual(solution2,result2)

class TestFileloadYaml(unittest.TestCase):
    """Test the load of a YAML file.

    Methods
    -------
    setUp():
        Set up the YAML file fullpath.
    test_load_yaml():
        Test the corrent loading of a YAML file with and without required keys.
    test_load_yaml_exception()
        Test that when the YAML file does not contains the required keys it
        should raise a ValueError exception.
    """
    def setUp(self):
        """Set up the YAML file fullpath."""
        file_name = 'load_files.yaml'
        self.file = os.path.join(test_dir,file_name)

    def test_load_yaml(self):
        """Test the corrent loading of a YAML file with and without required keys."""
        yaml_file = fileload.load_yaml_file(self.file)
        solution = ['database','pre_ingest','datafiles']
        result = list(yaml_file.keys())
        result2 = list(fileload.load_yaml_file(
            self.file,solution).keys())
        self.assertEqual(solution,result)
        self.assertEqual(solution,result2)

    def test_load_yaml_exceptions(self):
        """Test that when the YAML file does not contains the required keys it
        should raise a ValueError exception.
        """
        required_keys = ['neo4j_user','neo4j_password','neo4j_uri','database']
        with self.assertRaises(ValueError):
            fileload.load_yaml_file(self.file,required_keys)

if __name__ == '__main__':
    unittest.main()
