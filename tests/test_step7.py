import unittest
import sys
from pathlib import Path

import pandas as pd
import sqlite3
sys.path.insert(1, str(Path(__file__).parents[1]))


from mini_project2 import step7_create_productcategory_table 


class TestMethods(unittest.TestCase):
 
    def test_1(self):
        normalized_database_filename = 'normalized.db'
        data_filename = 'data.csv'
        step7_create_productcategory_table(data_filename, normalized_database_filename)
        data = pd.read_csv("step7.csv")
        conn = sqlite3.connect(normalized_database_filename)
        df = pd.read_sql_query("""SELECT * FROM ProductCategory""", conn)
        assert df.equals(data) == True
        conn.close()

 
if __name__ == '__main__':
    unittest.main()
