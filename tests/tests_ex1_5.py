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
               
    def test_ex1_1(self):
        sql_statement = mini_project2.ex1(self.conn, 'Alejandra Camino')
        data = pd.read_csv("ex1_1.csv")
        df = pd.read_sql_query(sql_statement, self.conn)
        assert df.equals(data) == True

    def test_ex1_2(self):
        sql_statement = mini_project2.ex1(self.conn, 'Eduardo Saavedra')
        data = pd.read_csv("ex1_2.csv")
        df = pd.read_sql_query(sql_statement, self.conn)
        assert df.equals(data) == True


    def test_ex2_1(self):
        sql_statement = mini_project2.ex2(self.conn, 'Alejandra Camino')
        data = pd.read_csv("ex2_1.csv")
        df = pd.read_sql_query(sql_statement, self.conn)
        assert df.equals(data) == True

    def test_ex2_2(self):
        sql_statement = mini_project2.ex2(self.conn, 'Eduardo Saavedra')
        data = pd.read_csv("ex2_2.csv")
        df = pd.read_sql_query(sql_statement, self.conn)
        assert df.equals(data) == True

    def test_ex3(self):
        sql_statement = mini_project2.ex3(self.conn)
        data = pd.read_csv("ex3.csv")
        df = pd.read_sql_query(sql_statement, self.conn)
        assert df.equals(data) == True

    def test_ex4(self):
        sql_statement = mini_project2.ex4(self.conn)
        data = pd.read_csv("ex4.csv")
        df = pd.read_sql_query(sql_statement, self.conn)
        assert df.equals(data) == True

    def test_ex5(self):
        sql_statement = mini_project2.ex5(self.conn)
        data = pd.read_csv("ex5.csv")
        df = pd.read_sql_query(sql_statement, self.conn)
        assert df.equals(data) == True

    
    @classmethod
    def tearDownClass(cls):
      cls.conn.close()
  
if __name__ == '__main__':
    unittest.main()
