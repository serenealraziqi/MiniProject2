import unittest
import sys
from pathlib import Path

import pandas as pd
import sqlite3
sys.path.insert(1, str(Path(__file__).parents[1]))


import mini_project2

class TestMethods(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = sqlite3.connect("normalized.db")
               
    def test_ex6(self):
        sql_statement = mini_project2.ex6(self.conn)
        data = pd.read_csv("ex6.csv")
        df = pd.read_sql_query(sql_statement, self.conn)
        assert df.equals(data) == True

    def test_ex7(self):
        sql_statement = mini_project2.ex7(self.conn)
        data = pd.read_csv("ex7.csv")
        df = pd.read_sql_query(sql_statement, self.conn)
        assert df.equals(data) == True


    def test_ex8(self):
        sql_statement = mini_project2.ex8(self.conn)
        data = pd.read_csv("ex8.csv")
        df = pd.read_sql_query(sql_statement, self.conn)
        assert df.equals(data) == True

    def test_ex9(self):
        sql_statement = mini_project2.ex9(self.conn)
        data = pd.read_csv("ex9.csv")
        df = pd.read_sql_query(sql_statement, self.conn)
        assert df.equals(data) == True

    def test_ex10(self):
        sql_statement = mini_project2.ex10(self.conn)
        data = pd.read_csv("ex10.csv")
        df = pd.read_sql_query(sql_statement, self.conn)
        assert df.equals(data) == True

    def test_ex11(self):
        sql_statement = mini_project2.ex11(self.conn)
        data = pd.read_csv("ex11.csv")
        df = pd.read_sql_query(sql_statement, self.conn)
        assert df.equals(data) == True

   
    @classmethod
    def tearDownClass(cls):
      cls.conn.close()
  
if __name__ == '__main__':
    unittest.main()
