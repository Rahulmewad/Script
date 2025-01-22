#!/usr/bin/env python
# coding: utf-8



"""ETL process completed:
    
1.  Load CSV data into a pandas DataFrame
2.  Data transformation (if needed)
3.  You can perform any data cleaning, filtering, or transformation here.
4.  Connect to the PostgreSQL database
5.  Create the data table if it doesn't exist
6.  Insert data into the PostgreSQL database
7.  Close the database connection """


#pip install pandas psycopg2
import pandas as pd
import psycopg2 # type: ignore

# Replace these with your database connection details
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = '5432'

# File paths
csv_file_path = 'buymie_2.csv'

# Load CSV data into a pandas DataFrame
df = pd.read_csv(csv_file_path)


# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cur = conn.cursor()

# Create the sales_data table if it doesn't exist
cur.execute("""
    CREATE TABLE IF NOT EXISTS Advarisk (
        ID SERIAL PRIMARY KEY,
        "Product_Name" TEXT,
        "Price" NUMERIC,
        "Offer_Price" NUMERIC
    )
""")
conn.commit()

# Insert data into the PostgreSQL database
for index, row in df.iterrows():
    cur.execute("""
        INSERT INTO Advarisk ("Product_Name", "Price", "Offer_Price")
        VALUES (%s, %s, %s)
    """, (row['Product_Name'], row['Price'], row['Offer_Price']))
    conn.commit()

# Close the database connection
cur.close()
conn.close()

print("ETL process completed.")


