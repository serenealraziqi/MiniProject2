import unittest
import sys
from pathlib import Path

import pandas as pd
import sqlite3
sys.path.insert(1, str(Path(__file__).parents[1]))


from mini_project2 import step10_create_product_to_productid_dictionary 


class TestMethods(unittest.TestCase):
 
    def test_1(self):
        normalized_database_filename = 'normalized.db'
        product_to_productid_dict = step10_create_product_to_productid_dictionary(normalized_database_filename)
        expected_solution = {
            'Alice Mutton': 1,
            'Aniseed Syrup': 2,
            'Boston Crab Meat': 3,
            'Camembert Pierrot': 4,
            'Carnarvon Tigers': 5,
            'Chai': 6,
            'Chang': 7,
            'Chartreuse verte': 8,
            "Chef Anton's Cajun Seasoning": 9,
            "Chef Anton's Gumbo Mix": 10,
            'Chocolade': 11,
            'Cote de Blaye': 12,
            'Escargots de Bourgogne': 13,
            'Filo Mix': 14,
            'Flotemysost': 15,
            'Geitost': 16,
            'Genen Shouyu': 17,
            'Gnocchi di nonna Alice': 18,
            'Gorgonzola Telino': 19,
            "Grandma's Boysenberry Spread": 20,
            'Gravad lax': 21,
            'Guarana Fantastica': 22,
            'Gudbrandsdalsost': 23,
            'Gula Malacca': 24,
            'Gumbar Gummibarchen': 25,
            "Gustaf's Knackebrod": 26,
            'Ikura': 27,
            'Inlagd Sill': 28,
            'Ipoh Coffee': 29,
            "Jack's New England Clam Chowder": 30,
            'Konbu': 31,
            'Lakkalikoori': 32,
            'Laughing Lumberjack Lager': 33,
            'Longlife Tofu': 34,
            'Louisiana Fiery Hot Pepper Sauce': 35,
            'Louisiana Hot Spiced Okra': 36,
            'Manjimup Dried Apples': 37,
            'Mascarpone Fabioli': 38,
            'Maxilaku': 39,
            'Mishi Kobe Niku': 40,
            'Mozzarella di Giovanni': 41,
            'Nord-Ost Matjeshering': 42,
            'Northwoods Cranberry Sauce': 43,
            'NuNuCa Nu-Nougat-Creme': 44,
            'Original Frankfurter grune Soe': 45,
            'Outback Lager': 46,
            'Pate chinois': 47,
            'Pavlova': 48,
            'Perth Pasties': 49,
            'Queso Cabrales': 50,
            'Queso Manchego La Pastora': 51,
            'Raclette Courdavault': 52,
            'Ravioli Angelo': 53,
            'Rhonbrau Klosterbier': 54,
            'Rod Kaviar': 55,
            'Rogede sild': 56,
            'Rossle Sauerkraut': 57,
            'Sasquatch Ale': 58,
            'Schoggi Schokolade': 59,
            'Scottish Longbreads': 60,
            'Singaporean Hokkien Fried Mee': 61,
            "Sir Rodney's Marmalade": 62,
            "Sir Rodney's Scones": 63,
            "Sirop d'erable": 64,
            'Spegesild': 65,
            'Steeleye Stout': 66,
            'Tarte au sucre': 67,
            'Teatime Chocolate Biscuits': 68,
            'Thuringer Rostbratwurst': 69,
            'Tofu': 70,
            'Tourtiere': 71,
            'Tunnbrod': 72,
            "Uncle Bob's Organic Dried Pears": 73,
            'Valkoinen suklaa': 74,
            'Vegie-spread': 75,
            'Wimmers gute Semmelknodel': 76,
            'Zaanse koeken': 77
        }
        assert expected_solution == product_to_productid_dict

 
if __name__ == '__main__':
    unittest.main()
