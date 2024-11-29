import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
from pathlib import Path

def create_database(conn, dbname):
    with conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{dbname}'")
        exists = cur.fetchone()
        if exists:
            print(f"Database '{dbname}' already exists.")
            for _ in range(5):
                input("Press Enter to continue...")
            cur.execute(f"DROP DATABASE {dbname}")
            print(f"Database '{dbname}' dropped.")
        cur.execute(f"CREATE DATABASE {dbname}")
        print(f"Database '{dbname}' created.")

def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE location (
                LocationID SERIAL PRIMARY KEY,
                Location VARCHAR(255)
            );
        """)
        cur.execute("""
            CREATE TABLE properties (
                PropertyID SERIAL PRIMARY KEY,
                Name VARCHAR(255),
                Title VARCHAR(255),
                Description TEXT,
                LocationID INT,
                Total_Area NUMERIC,
                FOREIGN KEY (LocationID) REFERENCES location(LocationID)
            );
        """)
        cur.execute("""
            CREATE TABLE features (
                FeatureID SERIAL PRIMARY KEY,
                Baths INT,
                Balcony BOOLEAN,
                PropertyID INT,
                FOREIGN KEY (PropertyID) REFERENCES properties(PropertyID)
            );
        """)
        cur.execute("""
            CREATE TABLE pricing (
                PriceID SERIAL PRIMARY KEY,
                Price TEXT,
                Price_per_SQFT NUMERIC,
                PropertyID INT,
                FOREIGN KEY (PropertyID) REFERENCES properties(PropertyID)
            );
        """)
        conn.commit()

def populate_source_3(conn, base_dir):
    features_df = pd.read_csv(base_dir / 'normalized/features.csv')
    location_df = pd.read_csv(base_dir / 'normalized/location.csv')
    pricing_df = pd.read_csv(base_dir / 'normalized/pricing.csv')
    properties_df = pd.read_csv(base_dir / 'normalized/properties.csv')

    with conn.cursor() as cur:
        for _, row in location_df.iterrows():
            cur.execute("INSERT INTO location (LocationID, Location) VALUES (%s, %s)", 
                        (int(row['LocationID']) if not pd.isna(row['LocationID']) else None, 
                         row['Location']))

        for _, row in properties_df.iterrows():
            cur.execute("""
                INSERT INTO properties (PropertyID, Name, Title, Description, LocationID, Total_Area)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                int(row['PropertyID']) if not pd.isna(row['PropertyID']) else None, 
                row['Name'], 
                row['Title'], 
                row['Description'], 
                int(row['LocationID']) if not pd.isna(row['LocationID']) else None, 
                float(row['Total_Area']) if not pd.isna(row['Total_Area']) else None))

        for _, row in features_df.iterrows():
            cur.execute("INSERT INTO features (FeatureID, Baths, Balcony, PropertyID) VALUES (%s, %s, %s, %s)", 
                        (int(row['FeatureID']) if not pd.isna(row['FeatureID']) else None, 
                         int(row['Baths']) if not pd.isna(row['Baths']) else None, 
                         bool(row['Balcony']) if not pd.isna(row['Balcony']) else None, 
                         int(row['PropertyID']) if not pd.isna(row['PropertyID']) else None))

        for _, row in pricing_df.iterrows():
            cur.execute("INSERT INTO pricing (PriceID, Price, Price_per_SQFT, PropertyID) VALUES (%s, %s, %s, %s)", 
                        (int(row['PriceID']) if not pd.isna(row['PriceID']) else None, 
                         row['Price'], 
                         float(row['Price_per_SQFT']) if not pd.isna(row['Price_per_SQFT']) else None, 
                         int(row['PropertyID']) if not pd.isna(row['PropertyID']) else None))

        conn.commit()

dbname = "real_estate_db_source_3"
user = "postgres"
password = "admin"
host = "localhost"
port = "5432"

# Connect to default postgres database to create new database
conn = psycopg2.connect(
    dbname="postgres",
    user=user,
    password=password,
    host=host,
    port=port
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

try:
    create_database(conn, dbname)
finally:
    conn.close()

# Connect to newly created database
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)

try:
    create_tables(conn)
    base_dir = Path("data/source 3")
    populate_source_3(conn, base_dir)
finally:
    conn.close()

# find all nulls

# SELECT 
#     COUNT(*) AS total_records,
#     COUNT(CASE WHEN Price IS NULL THEN 1 END) AS null_price,
#     COUNT(CASE WHEN Price_per_SQFT IS NULL THEN 1 END) AS null_price_per_sqft,
#     COUNT(CASE WHEN PropertyID IS NULL THEN 1 END) AS null_property_id
# FROM public.pricing;

# SELECT 
#     COUNT(*) AS total_records,
#     COUNT(CASE WHEN Baths IS NULL THEN 1 END) AS null_baths,
#     COUNT(CASE WHEN Balcony IS NULL THEN 1 END) AS null_balcony,
#     COUNT(CASE WHEN PropertyID IS NULL THEN 1 END) AS null_property_id
# FROM public.features;

# SELECT 
#     COUNT(*) AS total_records,
#     COUNT(CASE WHEN Name IS NULL THEN 1 END) AS null_name,
#     COUNT(CASE WHEN Title IS NULL THEN 1 END) AS null_title,
#     COUNT(CASE WHEN Description IS NULL THEN 1 END) AS null_description,
#     COUNT(CASE WHEN LocationID IS NULL THEN 1 END) AS null_location_id,
#     COUNT(CASE WHEN Total_Area IS NULL THEN 1 END) AS null_total_area
# FROM public.properties;

# SELECT 
#     COUNT(*) AS total_records,
#     COUNT(CASE WHEN Location IS NULL THEN 1 END) AS null_location
# FROM public.location;