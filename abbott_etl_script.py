# ===================================================================================
# Abbott Rapid Diagnostics - Sales & Marketing ETL Pipeline
# ===================================================================================
#
# Author: Your Name
# Project: Abbott Rapid Diagnostics Sales Analysis
#
# Description:
# This script performs a full ETL (Extract, Transform, Load) process for Abbott.
# 1. GENERATES 15,000 rows of realistic, messy sales data for diagnostic kits
#    over a 3-year period (2021-2023), including seasonality.
# 2. CLEANS and TRANSFORMS the data using the pandas library.
# 3. CREATES a comprehensive calendar dimension table.
# 4. LOADS the cleaned, normalized data into a MySQL database, creating a
#    star schema ready for BI analysis.
#
# ===================================================================================

import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error
import random
from datetime import datetime, timedelta
import json
import os

print("Abbott ETL Script Started...")

# --- PART 1: DATA GENERATION ---
# ===================================================================================

def generate_realistic_abbott_data(num_rows=15000):
    """
    Generates a Pandas DataFrame with messy, realistic sales data for Abbott.
    """
    print(f"Step 1: Generating {num_rows} rows of messy sales data (2021-2023)...")
    
    products = [
        {'id': 'P001', 'name': 'Panbio COVID-19 Ag Rapid Test', 'line': 'COVID-19', 'cost': 4.50},
        {'id': 'P002', 'name': 'BinaxNOW COVID-19 Ag Card', 'line': 'COVID-19', 'cost': 5.00},
        {'id': 'P003', 'name': 'ID NOW COVID-19', 'line': 'COVID-19', 'cost': 35.00},
        {'id': 'P004', 'name': 'BinaxNOW Influenza A&B Card', 'line': 'Influenza', 'cost': 15.20},
        {'id': 'P005', 'name': 'ID NOW Influenza A&B 2', 'line': 'Influenza', 'cost': 40.00},
        {'id': 'P006', 'name': 'BinaxNOW Strep A Test', 'line': 'Strep A', 'cost': 8.75},
        {'id': 'P007', 'name': 'Panbio COVID-19/Flu A&B', 'line': 'COVID-19', 'cost': 18.00},
    ]

    customers = [
        {'id': 'C101', 'name': 'City General Hospital', 'type': 'Hospital', 'region': 'Northeast'},
        {'id': 'C102', 'name': 'State Health Department', 'type': 'Government', 'region': 'South'},
        {'id': 'C103', 'name': 'County Medical Clinic', 'type': 'Clinic', 'region': 'West'},
        {'id': 'C104', 'name': 'MedSupply Distributors', 'type': 'Distributor', 'region': 'Midwest'},
        {'id': 'C105', 'name': 'Metro Health System', 'type': 'Hospital', 'region': 'Northeast'},
        {'id': 'C106', 'name': 'Rural Care Clinics', 'type': 'Clinic', 'region': 'South'},
        {'id': 'C107', 'name': 'Federal Health Agency', 'type': 'Government', 'region': 'West'},
        {'id': 'C108', 'name': 'Prime Diagnostics Inc', 'type': 'Distributor', 'region': 'Midwest'},
    ]
    
    sales_reps = [
        {'id': 'S501', 'name': 'John Smith', 'region': 'Northeast'},
        {'id': 'S502', 'name': 'Maria Garcia', 'region': 'South'},
        {'id': 'S503', 'name': 'David Chen', 'region': 'West'},
        {'id': 'S504', 'name': 'Emily White', 'region': 'Midwest'},
        {'id': 'S505', 'name': 'Sarah Brown', 'region': 'Northeast'},
        {'id': 'S506', 'name': 'Carlos Rodriguez', 'region': 'South'},
    ]

    data = []
    start_date = datetime(2021, 1, 1)
    end_date = datetime(2023, 12, 31)
    
    for i in range(num_rows):
        sale_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        product = random.choice(products)
        customer = random.choice(customers)
        sales_rep = random.choice([rep for rep in sales_reps if rep['region'] == customer['region']])

        # --- Simulate Seasonality and Trends ---
        quantity = random.randint(50, 200)
        # COVID test sales higher in 2021 and winters
        if 'COVID-19' in product['name']:
            if sale_date.year == 2021: quantity *= 3.0
            elif sale_date.year == 2022: quantity *= 1.5
            if sale_date.month in [1, 2, 11, 12]: quantity *= 1.5
        # Flu test sales higher in winters
        if 'Influenza' in product['name'] and sale_date.month in [1, 2, 3, 10, 11, 12]:
            quantity *= 2.0

        # --- Introduce Messiness ---
        unit_price = product['cost'] * random.uniform(1.2, 1.5)
        if i % 20 == 0: unit_price = np.nan  # Missing price
        if i % 50 == 0: quantity = -quantity # Return

        row = {
            'SaleID': f'SALE-{20210000 + i}',
            'SaleDate': sale_date.strftime('%Y-%m-%d'),
            'ProductID': product['id'],
            'ProductName': product['name'] if i % 10 != 0 else product['name'].upper(), # Inconsistent casing
            'ProductLine': product['line'],
            'CustomerID': customer['id'],
            'CustomerName': customer['name'],
            'CustomerType': customer['type'],
            'Region': customer['region'],
            'SalespersonID': sales_rep['id'],
            'SalespersonName': sales_rep['name'],
            'Quantity': int(quantity),
            'UnitPrice': round(unit_price, 2)
        }
        data.append(row)
        
    print("Data generation complete.")
    return pd.DataFrame(data)

# --- PART 2: DATA CLEANING & TRANSFORMATION ---
# ===================================================================================

def clean_and_transform_data(df):
    """
    Cleans and transforms the raw sales DataFrame.
    """
    print("\nStep 2: Cleaning and transforming data...")
    
    # 1. Standardize text fields
    df['ProductName'] = df['ProductName'].str.title()
    
    # 2. Handle missing UnitPrice
    # Impute missing prices with the average price for that specific product
    df['UnitPrice'] = df.groupby('ProductID')['UnitPrice'].transform(lambda x: x.fillna(x.mean()))
    df['UnitPrice'] = df['UnitPrice'].round(2)
    
    # 3. Create Calculated Columns
    df['SaleAmount'] = df['Quantity'] * df['UnitPrice']
    
    # 4. Handle returns (negative quantity)
    df['IsReturn'] = df['Quantity'] < 0
    df['Quantity'] = df['Quantity'].abs() # Make quantity positive for all records

    print("Data cleaning complete.")
    return df

# --- PART 3: DATABASE LOADING (ETL - Load) ---
# ===================================================================================

def get_db_connection(creds_path):
    """
    Reads credentials from a JSON file and returns a MySQL connection object.
    """
    print("\nStep 3.1: Connecting to MySQL database...")
    try:
        with open(creds_path, 'r') as f:
            creds = json.load(f)
        
        connection = mysql.connector.connect(
            host=creds['host'], user=creds['user'], password=creds['password'], database=creds['database']
        )
        if connection.is_connected():
            print("MySQL connection successful.")
            return connection
    except FileNotFoundError:
        print(f"ERROR: Credentials file not found at '{creds_path}'")
        return None
    except Error as e:
        print(f"ERROR: Could not connect to MySQL database: {e}")
        return None

def create_database_schema(cursor):
    """
    Creates the normalized star schema tables if they don't already exist.
    """
    print("Step 3.2: Creating database schema (if not exists)...")
    
    tables = {
        "dim_products": """
            CREATE TABLE IF NOT EXISTS dim_products (
                ProductID VARCHAR(50) PRIMARY KEY,
                ProductName VARCHAR(255) NOT NULL,
                ProductLine VARCHAR(100)
            ) ENGINE=InnoDB;
        """,
        "dim_customers": """
            CREATE TABLE IF NOT EXISTS dim_customers (
                CustomerID VARCHAR(50) PRIMARY KEY,
                CustomerName VARCHAR(255) NOT NULL,
                CustomerType VARCHAR(100),
                Region VARCHAR(100)
            ) ENGINE=InnoDB;
        """,
        "dim_salespeople": """
            CREATE TABLE IF NOT EXISTS dim_salespeople (
                SalespersonID VARCHAR(50) PRIMARY KEY,
                SalespersonName VARCHAR(255) NOT NULL,
                Region VARCHAR(100)
            ) ENGINE=InnoDB;
        """,
        "dim_dates": """
            CREATE TABLE IF NOT EXISTS dim_dates (
                DateKey INT PRIMARY KEY,
                FullDate DATE NOT NULL,
                Year INT,
                Quarter INT,
                MonthNumber INT,
                MonthName VARCHAR(20),
                WeekNumber INT,
                DayOfMonth INT,
                DayOfWeekName VARCHAR(20),
                IsWeekend BOOLEAN,
                UNIQUE KEY full_date_unique (FullDate)
            ) ENGINE=InnoDB;
        """,
        "fact_sales": """
            CREATE TABLE IF NOT EXISTS fact_sales (
                SaleFactID INT AUTO_INCREMENT PRIMARY KEY,
                SaleID VARCHAR(50) UNIQUE NOT NULL,
                DateKey INT,
                ProductID VARCHAR(50),
                CustomerID VARCHAR(50),
                SalespersonID VARCHAR(50),
                Quantity INT,
                UnitPrice DECIMAL(10, 2),
                SaleAmount DECIMAL(12, 2),
                IsReturn BOOLEAN,
                FOREIGN KEY (DateKey) REFERENCES dim_dates(DateKey),
                FOREIGN KEY (ProductID) REFERENCES dim_products(ProductID),
                FOREIGN KEY (CustomerID) REFERENCES dim_customers(CustomerID),
                FOREIGN KEY (SalespersonID) REFERENCES dim_salespeople(SalespersonID)
            ) ENGINE=InnoDB;
        """
    }
    
    try:
        for table_name, table_sql in tables.items():
            print(f"Creating table {table_name}...")
            cursor.execute(table_sql)
        print("Schema creation successful.")
    except Error as e:
        print(f"ERROR: Could not create schema: {e}")

def load_dimensions(cursor, conn, df):
    """
    Loads data into all dimension tables from the DataFrame.
    """
    print("Step 3.3: Loading dimension tables...")
    try:
        # dim_products
        products = df[['ProductID', 'ProductName', 'ProductLine']].drop_duplicates()
        cursor.executemany("INSERT IGNORE INTO dim_products VALUES (%s, %s, %s)", products.values.tolist())
        
        # dim_customers
        customers = df[['CustomerID', 'CustomerName', 'CustomerType', 'Region']].drop_duplicates()
        cursor.executemany("INSERT IGNORE INTO dim_customers VALUES (%s, %s, %s, %s)", customers.values.tolist())

        # dim_salespeople
        salespeople = df[['SalespersonID', 'SalespersonName', 'Region']].drop_duplicates()
        cursor.executemany("INSERT IGNORE INTO dim_salespeople VALUES (%s, %s, %s)", salespeople.values.tolist())

        # *** Comprehensive dim_dates ***
        df['SaleDate'] = pd.to_datetime(df['SaleDate'])
        min_date, max_date = df['SaleDate'].min(), df['SaleDate'].max()
        date_range = pd.date_range(start=min_date, end=max_date, freq='D')
        
        date_dim_df = pd.DataFrame({'FullDate': date_range})
        date_dim_df['DateKey'] = date_dim_df['FullDate'].dt.strftime('%Y%m%d').astype(int)
        date_dim_df['Year'] = date_dim_df['FullDate'].dt.year
        date_dim_df['Quarter'] = date_dim_df['FullDate'].dt.quarter
        date_dim_df['MonthNumber'] = date_dim_df['FullDate'].dt.month
        date_dim_df['MonthName'] = date_dim_df['FullDate'].dt.strftime('%B')
        date_dim_df['WeekNumber'] = date_dim_df['FullDate'].dt.isocalendar().week
        date_dim_df['DayOfMonth'] = date_dim_df['FullDate'].dt.day
        date_dim_df['DayOfWeekName'] = date_dim_df['FullDate'].dt.strftime('%A')
        date_dim_df['IsWeekend'] = date_dim_df['FullDate'].dt.dayofweek >= 5 # Saturday=5, Sunday=6
        
        # Reorder columns to match table schema
        date_dim_df = date_dim_df[['DateKey', 'FullDate', 'Year', 'Quarter', 'MonthNumber', 'MonthName', 'WeekNumber', 'DayOfMonth', 'DayOfWeekName', 'IsWeekend']]
        cursor.executemany("INSERT IGNORE INTO dim_dates VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", date_dim_df.values.tolist())
        
        conn.commit()
        print("Dimension tables loaded successfully.")
    except Error as e:
        print(f"ERROR: Could not load dimension tables: {e}")
        conn.rollback()

def load_fact_table(cursor, conn, df):
    """
    Loads data into the fact_sales table.
    """
    print("Step 3.4: Loading fact table...")
    try:
        df['DateKey'] = pd.to_datetime(df['SaleDate']).dt.strftime('%Y%m%d').astype(int)
        fact_df = df[['SaleID', 'DateKey', 'ProductID', 'CustomerID', 'SalespersonID', 'Quantity', 'UnitPrice', 'SaleAmount', 'IsReturn']]
        
        cursor.executemany("INSERT IGNORE INTO fact_sales VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s)", fact_df.values.tolist())
        
        conn.commit()
        print(f"Fact table loaded successfully. {cursor.rowcount} new rows inserted.")

    except Error as e:
        print(f"ERROR: Could not load fact table: {e}")
        conn.rollback()

# --- MAIN EXECUTION ---
# ===================================================================================

if __name__ == "__main__":
    
    # Define credentials path
    creds_file_path = "mysql_creds.json"
    
    # Run ETL Process
    raw_df = generate_realistic_abbott_data(num_rows=15000)
    cleaned_df = clean_and_transform_data(raw_df)
    
    print(f"\nINFO: Number of cleaned rows to be loaded: {len(cleaned_df)}")

    connection = get_db_connection(creds_file_path)
    
    if connection and connection.is_connected():
        cursor = connection.cursor()
        create_database_schema(cursor)
        load_dimensions(cursor, connection, cleaned_df)
        load_fact_table(cursor, connection, cleaned_df)
        cursor.close()
        connection.close()
        print("\nDatabase connection closed.")

    print("\nAbbott ETL Script Finished.")
