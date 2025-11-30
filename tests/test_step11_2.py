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
        conn = sqlite3.connect(normalized_database_filename)
        df = pd.read_sql_query("""SELECT count(*) COUNT FROM OrderDetail""", conn)
        assert df['COUNT'][0] == 621806
        conn.close()

 
if __name__ == '__main__':
    unittest.main()
