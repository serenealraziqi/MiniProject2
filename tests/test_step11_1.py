import unittest
import sys
from pathlib import Path

import pandas as pd
import sqlite3
sys.path.insert(1, str(Path(__file__).parents[1]))


from mini_project2 import step11_create_orderdetail_table 


class TestMethods(unittest.TestCase):
 
    def test_1(self):
        normalized_database_filename = 'normalized.db'
        data_filename = 'data.csv'
        step11_create_orderdetail_table(data_filename, normalized_database_filename)
        data = pd.read_csv("step11.csv")
        conn = sqlite3.connect(normalized_database_filename)
        df = pd.read_sql_query("""SELECT * FROM OrderDetail LIMIT 1000""", conn)

        assert df.equals(data) == True
        conn.close()
 
if __name__ == '__main__':
    unittest.main()
