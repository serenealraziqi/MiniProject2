import unittest
import sys
from pathlib import Path

import pandas as pd
import sqlite3
sys.path.insert(1, str(Path(__file__).parents[1]))


from mini_project2 import step8_create_productcategory_to_productcategoryid_dictionary 


class TestMethods(unittest.TestCase):
 
    def test_1(self):
        normalized_database_filename = 'normalized.db'
        productcategory_to_productcategoryid_dict = step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)
        expected_solution = {
            'Beverages': 1,
            'Condiments': 2,
            'Confections': 3,
            'Dairy Products': 4,
            'Grains/Cereals': 5,
            'Meat/Poultry': 6,
            'Produce': 7,
            'Seafood': 8
        }
        assert expected_solution == productcategory_to_productcategoryid_dict

 
if __name__ == '__main__':
    unittest.main()
