import unittest
import sys
from pathlib import Path

import pandas as pd
import sqlite3
sys.path.insert(1, str(Path(__file__).parents[1]))


from mini_project2 import step6_create_customer_to_customerid_dictionary 


class TestMethods(unittest.TestCase):
 
    def test_1(self):
        normalized_database_filename = 'normalized.db'
        customer_to_customerid_dict = step6_create_customer_to_customerid_dictionary(normalized_database_filename)
        expected_solution = {
            'Alejandra Camino': 1,
            'Alexander Feuer': 2,
            'Ana Trujillo': 3,
            'Anabela Domingues': 4,
            'Andre Fonseca': 5,
            'Ann Devon': 6,
            'Annette Roulet': 7,
            'Antonio Moreno': 8,
            'Aria Cruz': 9,
            'Art Braunschweiger': 10,
            'Bernardo Batista': 11,
            'Carine Schmitt': 12,
            'Carlos Gonzalez': 13,
            'Carlos Hernandez': 14,
            'Catherine Dewey': 15,
            'Christina Berglund': 16,
            'Daniel Tonini': 17,
            'Diego Roel': 18,
            'Dominique Perrier': 19,
            'Eduardo Saavedra': 20,
            'Elizabeth Brown': 21,
            'Elizabeth Lincoln': 22,
            'Felipe Izquierdo': 23,
            'Fran Wilson': 24,
            'Francisco Chang': 25,
            'Frederique Citeaux': 26,
            'Georg Pipps': 27,
            'Giovanni Rovelli': 28,
            'Guillermo Fernandez': 29,
            'Hanna Moos': 30,
            'Hari Kumar': 31,
            'Helen Bennett': 32,
            'Helvetius Nagy': 33,
            'Henriette Pfalzheim': 34,
            'Horst Kloss': 35,
            'Howard Snyder': 36,
            'Isabel de Castro': 37,
            'Jaime Yorres': 38,
            'Janete Limeira': 39,
            'Janine Labrune': 40,
            'Jean Fresniere': 41,
            'John Steel': 42,
            'Jonas Bergulfsen': 43,
            'Jose Pavarotti': 44,
            'Jose Pedro Freyre': 45,
            'Jytte Petersen': 46,
            'Karin Josephs': 47,
            'Karl Jablonski': 48,
            'Laurence Lebihan': 49,
            'Lino Rodriguez': 50,
            'Liu Wong': 51,
            'Liz Nixon': 52,
            'Lucia Carvalho': 53,
            'Manuel Pereira': 54,
            'Maria Anders': 55,
            'Maria Larsson': 56,
            'Marie Bertrand': 57,
            'Mario Pontes': 58,
            'Martin Sommer': 59,
            'Martine Rance': 60,
            'Mary Saveley': 61,
            'Matti Karttunen': 62,
            'Maurizio Moroni': 63,
            'Michael Holz': 64,
            'Miguel Angel Paolino': 65,
            'Palle Ibsen': 66,
            'Paolo Accorti': 67,
            'Pascale Cartrain': 68,
            'Patricia McKenna': 69,
            'Patricio Simpson': 70,
            'Paul Henriot': 71,
            'Paula Parente': 72,
            'Paula Wilson': 73,
            'Pedro Afonso': 74,
            'Peter Franken': 75,
            'Philip Cramer': 76,
            'Pirkko Koskitalo': 77,
            'Renate Messner': 78,
            'Rene Phillips': 79,
            'Rita Muller': 80,
            'Roland Mendel': 81,
            'Sergio Gutierrez': 82,
            'Simon Crowther': 83,
            'Sven Ottlieb': 84,
            'Thomas Hardy': 85,
            'Victoria Ashworth': 86,
            'Yang Wang': 87,
            'Yoshi Latimer': 88,
            'Yoshi Tannamuri': 89,
            'Yvonne Moncada': 90,
            'Zbyszek Piestrzeniewicz': 91
        }

        assert expected_solution == customer_to_customerid_dict

 
if __name__ == '__main__':
    unittest.main()
