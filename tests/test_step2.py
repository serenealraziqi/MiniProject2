import unittest
import sys
from pathlib import Path

import pandas as pd
import sqlite3
sys.path.insert(1, str(Path(__file__).parents[1]))


from mini_project2 import step2_create_region_to_regionid_dictionary 


class TestMethods(unittest.TestCase):
 
    def test_1(self):
        normalized_database_filename = 'normalized.db'
        region_to_regionid_dict = step2_create_region_to_regionid_dictionary(
            normalized_database_filename)
        expected_solution = {
            'British Isles': 1,
            'Central America': 2,
            'Eastern Europe': 3,
            'North America': 4,
            'Northern Europe': 5,
            'Scandinavia': 6,
            'South America': 7,
            'Southern Europe': 8,
            'Western Europe': 9
        }
        assert expected_solution == region_to_regionid_dict

 
if __name__ == '__main__':
    unittest.main()
