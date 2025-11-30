 ### Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows

def step1_create_region_table(data_filename, normalized_database_filename):
  regions = set()
  with open(data_filename, 'r', encoding = 'utf-8') as file:
    next(file)
    for line in file:
      parts = line.strip().split('\t')
      if len(parts) > 4:
        region = parts[4].strip()
        if region:
          clean_region = " ".join(region.split()).title()
          regions.add(clean_region)
  sorted_regions = sorted(regions)
  conn = create_connection(normalized_database_filename)
  create_table_sql = """
  CREATE TABLE IF NOT EXISTS Region(
    RegionID INTEGER NOT NULL PRIMARY KEY,
    Region TEXT NOT NULL
  );
  """
  create_table(conn, create_table_sql, drop_table_name="Region")
  region_values = [(r,) for r in sorted_regions]
  try:
    with conn:
      cur=conn.cursor()
      cur.executemany("INSERT INTO Region (Region) VALUES (?);", region_values)
  except sqlite3.Error as e:
    print("Error insterting regions:", e)
  finally:
    conn.close()
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
# WRITE YOUR CODE HERE

def step2_create_region_to_regionid_dictionary(normalized_database_filename):
  conn = create_connection(normalized_database_filename)
  region_to_id ={}
  try:
    with conn:
      cur = conn.cursor()
      cur.execute("SELECT RegionID, Region FROM Region;")
      rows = cur.fetchall()
      for row in rows:
        region_id, region_name = row
        region_to_id[region_name] = region_id
  except sqlite3.Error as e:
    print("Error fetching regions:", e)
  finally:
    conn.close()
  return region_to_id
# WRITE YOUR CODE HERE


def step3_create_country_table(data_filename, normalized_database_filename):
  region_to_regionid_dict = step2_create_region_to_regionid_dictionary(normalized_database_filename)
  country_region_pairs = set()
  with open(data_filename, 'r', encoding ='utf-8') as file:
    next(file)
    for line in file:
      parts = line.strip().split('\t')
      if len(parts) > 4:
        country = parts[3].strip().title()
        region = parts[4].strip().title()
        if country and region:
          country_region_pairs.add((country, region))
  sorted_pairs = sorted(country_region_pairs, key = lambda x: x[0])
  conn = create_connection(normalized_database_filename)
  if conn is None:
    print("Error: cannot connect to database.")
    return
  create_table_sql = """
  CREATE TABLE IF NOT EXISTS Country(
    CountryID INTEGER NOT NULL PRIMARY KEY, 
    Country TEXT NOT NULL,
    RegionID INTEGER NOT NULL, 
    FOREIGN KEY (RegionID) REFERENCES Region(RegionID)

  );"""
  create_table(conn, create_table_sql, drop_table_name = "Country")
  country_values = []
  for country, region in sorted_pairs:
    region_id = region_to_regionid_dict.get(region)
    if region_id:
      country_values.append((country, region_id))
  try:
    with conn:
      cur = conn.cursor()
      cur.executemany("INSERT INTO Country (Country, RegionID) VALUES (?, ?);", country_values)
      conn.commit()
    print(f"{len(country_values)} countries inserted successfully.")
  except sqlite3.Error as e:
    print("Error iserting countries:", e)
  finally:
    conn.close()

def step4_create_country_to_countryid_dictionary(normalized_database_filename):
  conn = create_connection(normalized_database_filename)
  d = {}
  try:
    with conn:
      cur = conn.cursor()
      cur.execute("SELECT CountryID, Country FROM Country ORDER by CountryID;")
      for cid, country in cur.fetchall():
        clean_country = country.strip()
        if clean_country.lower() == "uk":
          clean_country = "UK"
        elif clean_country.lower() == "usa":
          clean_country = "USA"
        d[clean_country] = cid
  except sqlite3.Error as e:
    print("Error:", e)
  finally:
    conn.close()
  return d
        
        
def step5_create_customer_table(data_filename, normalized_database_filename):
  from mini_project2 import step4_create_country_to_countryid_dictionary
  data_entries=[]
  with open(data_filename, 'r', encoding = 'utf-8') as file:
    headers = next(file).strip().split('\t')
    try:
      idx_name = headers.index('Name')
      idx_address = headers.index('Address')
      idx_city = headers.index('City')
      idx_country = headers.index('Country')
    except ValueError:
      raise ValueError("Missing expected column headers")

    for record in file:
      record = record.strip().split('\t')
      if not record:
        continue
      
      given_name, surname = record[idx_name].split(' ', 1)
      street = record[idx_address]
      city = record[idx_city]
      country = record[idx_country]
      entry = (given_name, surname, street, city, country)
      data_entries.append(entry)
  sorted_entries = sorted(data_entries, key=lambda person: (person[0], person[1]))

  country_lookup = step4_create_country_to_countryid_dictionary(normalized_database_filename)
  prepared_customers = []
  for first, last, addr, city, country in sorted_entries:
    country_id = country_lookup[country]
    prepared_customers.append((first, last, addr, city, country_id))

  db_conn = create_connection(normalized_database_filename)
  table_sql = """
    CREATE TABLE IF NOT EXISTS Customer(
                  CustomerID INTEGER PRIMARY KEY,
                  FirstName TEXT, 
                  LastName TEXT, 
                  Address TEXT, 
                  City TEXT, 
                  CountryID INTEGER, 
                  FOREIGN KEY (CountryID) REFERENCES Country(CountryID)

  );"""
 
  insert_sql = """
  INSERT INTO Customer(FirstName, LastName, Address, City, CountryID)
  VALUES (?, ?, ?, ?, ?);"""

  with db_conn:
    create_table(db_conn, table_sql, drop_table_name='Customer')
    cur= db_conn.cursor()
    cur.executemany(insert_sql, prepared_customers)
    cur.close()




def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
  """
  CREAT a dictionary mapping 'FirstName LastName' -> CustomerID
  from the Customer table in the normalized database.
  """
  conn = sqlite3.connect(normalized_database_filename)
  cur = conn.cursor()

  cur.execute("""
  SELECT CustomerID, FirstName, LastName
  FROM Customer;
  """)
  rows = cur.fetchall()
  conn.close()

  name_to_customerid = {}
  for customer_id, first_name, last_name in rows:
    full_name = f"{first_name} {last_name}"
    name_to_customerid[full_name] = customer_id
  return name_to_customerid



def step7_create_productcategory_table(data_filename, normalized_database_filename):

  
  category_map=set()

  with open(data_filename, 'r', encoding='utf-8') as file:
    header = next(file).strip().split('\t')

    try:
      cat = header.index('ProductCategory')
      desc = header.index('ProductCategoryDescription')
    except ValueError:
      raise ValueError("Columns not here")
    for line in file:
      line = line.strip()
      if not line:
        continue
      parts = line.split('\t')
      categories = parts[cat].split(';')
      descriptions = parts[desc].split(';')
      for category, description in zip(categories, descriptions):
        category_map.add((category.strip(), description.strip()))

  
  sorted_categories = sorted(category_map)
  insert_row = []

  for category, description in sorted_categories:
     insert_row.append((category, description))
  create_table_sql = """
  CREATE TABLE IF NOT EXISTS ProductCategory(
    ProductCategoryID INTEGER NOT NULL PRIMARY KEY,
    ProductCategory TEXT NOT NULL,
    ProductCategoryDescription TEXT NOT NULL
  );"""
  insert_sql = """INSERT INTO ProductCategory(ProductCategory, ProductCategoryDescription) VALUES (?, ?);"""
  conn = create_connection(normalized_database_filename)

  with conn:
    create_table(conn, create_table_sql, drop_table_name = 'ProductCategory')
    cursor=conn.cursor()
    cursor.executemany(insert_sql, insert_row)
    cursor.close()
# WRITE YOUR CODE HERE

def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
  
  conn = sqlite3.connect(normalized_database_filename)
  cur = conn.cursor()
  cur.execute("SELECT ProductCategoryID, ProductCategory FROM ProductCategory;")
  rows = cur.fetchall()
  productcategory_dict = {category: category_id for category_id, category in rows}

  conn.close()
  return productcategory_dict

def step9_create_product_table(data_filename, normalized_database_filename):
  product_dict = step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)
  product_map = set()
  with open(data_filename, 'r', encoding ='utf-8') as file:
    header = next(file).strip().split('\t')
    try:
      prod_name = header.index('ProductName')
      prod_price = header.index('ProductUnitPrice')
      prod_cat = header.index('ProductCategory')
    except ValueError:
      raise ValueError("Columns not here")
    for line in file:
      line = line.strip()
      if not line:
        continue
      parts = line.split('\t')
      names = parts[prod_name].split(';')
      prices = parts[prod_price].split(';')
      categories = parts[prod_cat].split(';')

      for name, price, category in zip(names, prices, categories):
        name = name.strip()
        price = float(price.strip())
        category = category.strip()
        category_id = product_dict.get(category)
        if category_id:
          product_map.add((name, price, category_id))

  
  sorted_products = sorted(product_map, key=lambda x: x[0])
  insert_row = []

  create_product_sql = """
  CREATE TABLE IF NOT EXISTS Product(
    ProductID INTEGER PRIMARY KEY,
    ProductName TEXT NOT NULL,
    ProductUnitPrice REAL NOT NULL,
    ProductCategoryID INTEGER NOT NULL,
    FOREIGN KEY (ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID)
  );"""
  insert_sql = """INSERT INTO Product(ProductName, ProductUnitPrice, ProductCategoryID) VALUES (?, ?, ?);"""
  conn = create_connection(normalized_database_filename)

  with conn:
    create_table(conn, create_product_sql, drop_table_name = 'Product')
    cursor=conn.cursor()
    cursor.executemany(insert_sql, sorted_products)
    cursor.close()

def step10_create_product_to_productid_dictionary(normalized_database_filename):
  conn = sqlite3.connect(normalized_database_filename)
  cur = conn.cursor()
  cur.execute("SELECT ProductID, ProductName FROM Product;")
  rows = cur.fetchall()
  product_dict = {product: product_id for product_id, product in rows}

  conn.close()
  return product_dict

def step11_create_orderdetail_table(data_filename, normalized_database_filename):
  import datetime
  order_data = []


  with open(data_filename, 'r', encoding ='utf-8') as file:
    header = next(file).strip().split('\t')
    try:
      cust_name = header.index('Name')      
      prod_name = header.index('ProductName')
      order_date = header.index('OrderDate')
      quantity_orderded = header.index('QuantityOrderded')
    except ValueError:
      raise ValueError
      
    for line in file:    
      line = line.strip()
      if not line:
        continue
      parts = line.split('\t')
      customer_name = parts[cust_name].strip()
      product_name = parts[prod_name].strip().split(';')
      order_dates = parts[order_date].strip().split(';')
      quantity = parts[quantity_orderded].strip().split(';')

      for prod, quant, date in zip(product_name, quantity, order_dates):
        order_d = (customer_name, prod, quant, date)
        order_data.append(order_d)
  product_dict = step10_create_product_to_productid_dictionary(normalized_database_filename)
  customer_dict = step6_create_customer_to_customerid_dictionary(normalized_database_filename)

  rows = []
  for customer_name, product_name, quantity, order_date in order_data:
    cust_id =customer_dict[customer_name]
    prod_id = product_dict[product_name]
    format_date = datetime.datetime.strptime(order_date, '%Y%m%d').strftime('%Y-%m-%d')
    rows.append((cust_id, prod_id, format_date, int(quantity)))
  create_orderdetail_sql = """
  CREATE TABLE IF NOT EXISTS OrderDetail(
    OrderID INTEGER PRIMARY KEY,
    CustomerID INTEGER NOT NULL,
    ProductID INTEGER NOT NULL,
    OrderDate INTEGER NOT NULL,
    QuantityOrdered INTEGER NOT NULL,
    FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID),
    FOREIGN KEY (ProductID) REFERENCES Product(ProductID)    
  );"""
  insert_sql = """INSERT INTO OrderDetail(CustomerID, ProductID, OrderDate, QuantityOrdered) VALUES (?, ?, ?, ?);"""
  conn =create_connection(normalized_database_filename)

  with conn:
    create_table(conn, create_orderdetail_sql, drop_table_name='OrderDetail')
    cursor=conn.cursor()
    cursor.executemany(insert_sql, rows)
    cursor.close()

def ex1(conn, CustomerName):
  customer_dict = step6_create_customer_to_customerid_dictionary('normalized.db')
  customer_id = customer_dict[CustomerName]
  sql_statement = f"""
  SELECT
    c.FirstName || ' ' || c.LastName AS Name,
    p.ProductName AS ProductName,
    od.OrderDate AS OrderDate,
    p.ProductUnitPrice As ProductUnitPrice,
    od.QuantityOrdered As QuantityOrdered,
    ROUND(p.ProductUnitPrice * od.QuantityOrdered, 2) AS Total
  FROM OrderDetail od
  JOIN Customer c ON c.CustomerID = od.CustomerID
  JOIN Product p ON p.ProductID = od.ProductID
  WHERE od.CustomerID = {customer_id};
  """
  return sql_statement
    # Simply, you are fetching all the rows for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    

def ex2(conn, CustomerName):
  customer_dict = step6_create_customer_to_customerid_dictionary('normalized.db')
  customer_id = customer_dict[CustomerName]

    
    # Simply, you are summing the total for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
  sql_statement = f"""
    SELECT 
      c.FirstName || ' ' || c.LastName AS Name,
      ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered), 2) AS Total
    FROM OrderDetail od
    JOIN Customer c ON od.CustomerID = c.CustomerID
    JOIN Product p ON od.ProductID = p.ProductID
    WHERE od.CustomerID = {customer_id}
    GROUP BY c.FirstName, c.LastName;
    """
# WRITE YOUR CODE HERE
  return sql_statement

def ex3(conn):
    
    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    
  sql_statement = """
  SELECT
    c.FirstName || ' ' || c.LastName AS Name,
    ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered), 2) AS Total
  FROM OrderDetail od
  JOIN Customer c ON od.CustomerID = c.CustomerID
  JOIN Product p ON od.ProductID = p.ProductID
  GROUP BY c.CustomerID, c.FirstName, c.LastName
  ORDER BY Total DESC

    """
# WRITE YOUR CODE HERE
  return sql_statement
def ex4(conn):
    
    # Simply, find the total for all the region
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, Country, and 
    # Region tables.
    # Pull out the following columns. 
    # Region
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    
  sql_statement = """
  SELECT
    r.Region AS Region,
    ROUND(SUM(p.ProductUnitPrice * od.QuantityOrdered), 2) AS Total
  FROM OrderDetail od
  JOIN Customer c ON od.CustomerID = c.CustomerID
  JOIN Country co ON c.CountryID = co.CountryID
  JOIN Region r ON co.RegionID = r.RegionID
  JOIN Product p ON od.ProductID = p.ProductID
  GROUP BY r.Region
  ORDER BY Total DESC;
    """
# WRITE YOUR CODE HERE
  return sql_statement

def ex5(conn):
    
    # Simply, find the total for all the countries
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, and Country table.
    # Pull out the following columns. 
    # Country
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round
    # ORDER BY Total Descending 

  sql_statement = """
  SELECT 
    CASE 
      WHEN LENGTH(Country.Country) <= 3 THEN UPPER(Country.Country)
      ELSE Country.Country
    END AS Country,
    ROUND(SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered), 0) AS Total
  FROM OrderDetail
  JOIN Customer ON OrderDetail.CustomerID = Customer.CustomerID
  JOIN Country ON Customer.CountryID = Country.CountryID
  JOIN Product ON OrderDetail.ProductID = Product.ProductID
  GROUP BY Country.Country
  ORDER BY Total DESC;
  """
  df = pd.read_sql_query(sql_statement, conn)
  print(df)
# WRITE YOUR CODE HERE
  return sql_statement


def ex6(conn):
    
    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, TotalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region

    sql_statement = """
    SELECT
      Region.Region AS Region,
      CASE 
        WHEN LENGTH(Country.Country) <= 3 THEN UPPER(Country.Country)
        ELSE Country.Country
    END AS Country,
      ROUND(SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered), 0) AS CountryTotal,
      RANK() OVER (PARTITION By Region.Region ORDER BY SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered) DESC) AS TotalRank
    FROM OrderDetail
    JOIN Customer ON OrderDetail.CustomerID = Customer.CustomerID
    JOIN Country ON Customer.CountryID = Country.CountryID
    JOIN Product ON OrderDetail.ProductID = Product.ProductID
    JOIN Region ON Country.RegionID = Region.RegionID
    GROUP BY Region.Region, Country.Country
    ORDER BY Region.Region ASC, TotalRank ASC;
    """

# WRITE YOUR CODE HERE

    return sql_statement



def ex7(conn):
    
    # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, Total, TotalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"

    sql_statement = """
    WITH RankedCountries AS (
      SELECT Region.Region AS Region,       
      CASE 
        WHEN LENGTH(Country.Country) <= 3 THEN UPPER(Country.Country)
        ELSE Country.Country
      END AS Country,
      ROUND(SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered), 0) AS CountryTotal,
      RANK() OVER(PARTITION BY Region.Region ORDER BY SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered)DESC )AS CountryRegionalRank
      FROM OrderDetail
      JOIN Customer ON OrderDetail.CustomerID = Customer.CustomerID
      JOIN Product ON OrderDetail.ProductID = Product.ProductID
      JOIN Country ON Customer.CountryID = Country.CountryID
      JOIN Region ON Country.RegionID = Region.RegionID
      GROUP BY Region.Region, Country.Country
      )
    SELECT Region, Country, CountryTotal, CountryRegionalRank
    FROM RankedCountries
    WHERE CountryRegionalRank = 1
    ORDER BY Region ASC;
    """
# WRITE YOUR CODE HERE

    return sql_statement

def ex8(conn):
    
    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!

    sql_statement = """
    WITH CustomerSales AS(
      SELECT 
        CASE
          WHEN CAST(SUBSTR(OrderDetail.OrderDate,6,2)AS INTEGER) BETWEEN 1 AND 3 THEN 'Q1'
          WHEN CAST(SUBSTR(OrderDetail.OrderDate,6,2)AS INTEGER) BETWEEN 4 AND 6 THEN 'Q2'
          WHEN CAST(SUBSTR(OrderDetail.OrderDate,6,2)AS INTEGER) BETWEEN 7 AND 9 THEN 'Q3'
          ELSE 'Q4'
        END AS Quarter, 

        CAST(SUBSTR(OrderDetail.OrderDate, 1,4) AS INTEGER) AS Year,
        OrderDetail.CustomerID,
        ROUND(SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered)) AS Total
      FROM OrderDetail
      JOIN Product ON OrderDetail.ProductID = Product.ProductID
      GROUP BY Year, Quarter, OrderDetail.CustomerID
    )
    SELECT Quarter, Year, CustomerID, Total
    FROM CustomerSales
    ORDER BY Year;
    """
# WRITE YOUR CODE HERE
    df = pd.read_sql_query(sql_statement, conn)
    print(df)
    return sql_statement

def ex9(conn):
    
    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()

    sql_statement = """
    WITH CustomerSales AS(
      SELECT 
        CASE
          WHEN CAST(SUBSTR(OrderDetail.OrderDate,6,2)AS INTEGER) BETWEEN 1 AND 3 THEN 'Q1'
          WHEN CAST(SUBSTR(OrderDetail.OrderDate,6,2)AS INTEGER) BETWEEN 4 AND 6 THEN 'Q2'
          WHEN CAST(SUBSTR(OrderDetail.OrderDate,6,2)AS INTEGER) BETWEEN 7 AND 9 THEN 'Q3'
          ELSE 'Q4'
        END AS Quarter, 

        CAST(SUBSTR(OrderDetail.OrderDate, 1,4) AS INTEGER) AS Year,
        OrderDetail.CustomerID,
        ROUND(SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered)) AS Total
      FROM OrderDetail
      JOIN Product ON OrderDetail.ProductID = Product.ProductID
      GROUP BY Year, Quarter, OrderDetail.CustomerID
    ),
    RankedSales AS (
      SELECT Quarter, Year, CustomerID, Total,
      RANK() OVER (PARTITION BY Year, Quarter ORDER BY Total DESC) AS CustomerRank
    FROM CustomerSales
    )
    SELECT Quarter, Year, CustomerID, Total
    FROM RankedSales
    ORDER CustomerRank <= 5
    ORDER BY Year ASC, Quarter ASC, Total DESC;

    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex10(conn):
    
    # Rank the monthy sales
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total

    sql_statement = """
    """

# WRITE YOUR CODE HERE
    return sql_statement

def ex11(conn):
    
    # Find the MaxDaysWithoutOrder for each customer 
    # Output Columns: 
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate, 
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag
    sql_statement = """
    WITH OrderGaps AS (
      SELECT 
        Customer.CustomerID,
        Customer.FirstName,
        Customer.LastName,
        Country.Country,
        OrderDetail.OrderDate,
        LAS(OrderDetail.OrderDate) OVER (PARTITION BY Customer.CustomerID ORDER BY OrderDetail.OrderData) AS PreviousOrderDate
      FROM OrderDetail
      JOIN Customer ON OrderDetail.CustomerID = Customer.CustomerID
      JOIN Country ON Customer.CountryID = Country.CountryID
      ),
    DaysBetweenOrders AS ( 
      SELECT CustomerID, FirstName, LastName, Country, OrderDate, PreviousOrderDate,
        CASE
          WHEN PreviousOrderDate IS NULL THEN 0
          ELSE JULIANDAY(OrderDate) - JULIANDAY(PreviousOrderDate)
        END AS DaysWithoutOrder
      FROM OrderGaps
    
    )
    SELECT CustomerID, FirstName, LastName, Country, OrderDate, PreviousOrderDate, MAX(DaysWithoutOrder) AS MaxDaysWithoutOrder
    FROM DaysBetweenOrders
    GROUP BY CustomerID
    ORDER BY MaxDaysWithoutOrder DESC;
    )
    """
# WRITE YOUR CODE HERE
    return sql_statement
