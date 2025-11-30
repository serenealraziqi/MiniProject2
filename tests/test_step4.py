import unittest
import sys
from pathlib import Path

import pandas as pd
import sqlite3
sys.path.insert(1, str(Path(__file__).parents[1]))


from mini_project2 import step4_create_country_to_countryid_dictionary 


class TestMethods(unittest.TestCase):
 
    def test_1(self):
        normalized_database_filename = 'normalized.db'
        country_to_countryid_dict = step4_create_country_to_countryid_dictionary(
            normalized_database_filename)
        expected_solution = {
            'Argentina': 1,
            'Austria': 2,
            'Belgium': 3,
            'Brazil': 4,
            'Canada': 5,
            'Denmark': 6,
            'Finland': 7,
            'France': 8,
            'Germany': 9,
            'Ireland': 10,
            'Italy': 11,
            'Mexico': 12,
            'Norway': 13,
            'Poland': 14,
            'Portugal': 15,
            'Spain': 16,
            'Sweden': 17,
            'Switzerland': 18,
            'UK': 19,
            'USA': 20,
            'Venezuela': 21
        }
        assert expected_solution == country_to_countryid_dict

 
if __name__ == '__main__':
    unittest.main()
