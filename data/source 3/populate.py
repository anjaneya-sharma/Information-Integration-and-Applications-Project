import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import os
import platform

# Set dynamic paths for different operating systems
def get_library_path():
    system = platform.system()
    if system == "Darwin":  # macOS
        return "/usr/local/opt/libpq/lib"
    elif system == "Linux":
        return "/usr/lib/x86_64-linux-gnu"
    elif system == "Windows":
        return "C:\\Program Files\\PostgreSQL\\lib"
    else:
        raise Exception("Unsupported Operating System")

# Set library path for psycopg2 if needed
if platform.system() == "Darwin" or platform.system() == "Linux":
    lib_path = get_library_path()
    os.environ["LD_LIBRARY_PATH"] = lib_path

# Construct paths dynamically and check if files exist
def get_file_path(*paths):
    # Use the directory of the current script as the base directory
    base_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the absolute path using base_dir and provided paths
    file_path = os.path.join(base_dir, *paths)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    return file_path

# Create Database Function
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

# Create Tables Function
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

# Populate Data Function
def populate_source_3(conn):
    # Using get_file_path to construct paths dynamically and validate their existence
    features_path = get_file_path( 'normalized', 'features.csv')
    location_path = get_file_path('normalized', 'location.csv')
    pricing_path = get_file_path( 'normalized', 'pricing.csv')
    properties_path = get_file_path('normalized', 'properties.csv')
    
    # Load data from CSV files
    features_df = pd.read_csv(features_path)
    location_df = pd.read_csv(location_path)
    pricing_df = pd.read_csv(pricing_path)
    properties_df = pd.read_csv(properties_path)

    with conn.cursor() as cur:
        for _, row in location_df.iterrows():
            cur.execute("INSERT INTO location (LocationID, Location) VALUES (%s, %s)", (int(row['LocationID']) if not pd.isna(row['LocationID']) else None, row['Location']))

        for _, row in properties_df.iterrows():
            cur.execute("""
                INSERT INTO properties (PropertyID, Name, Title, Description, LocationID, Total_Area)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (int(row['PropertyID']) if not pd.isna(row['PropertyID']) else None, row['Name'], row['Title'], row['Description'], int(row['LocationID']) if not pd.isna(row['LocationID']) else None, float(row['Total_Area']) if not pd.isna(row['Total_Area']) else None))

        for _, row in features_df.iterrows():
            cur.execute("INSERT INTO features (FeatureID, Baths, Balcony, PropertyID) VALUES (%s, %s, %s, %s)", (int(row['FeatureID']) if not pd.isna(row['FeatureID']) else None, int(row['Baths']) if not pd.isna(row['Baths']) else None, bool(row['Balcony']) if not pd.isna(row['Balcony']) else None, int(row['PropertyID']) if not pd.isna(row['PropertyID']) else None))

        for _, row in pricing_df.iterrows():
            cur.execute("INSERT INTO pricing (PriceID, Price, Price_per_SQFT, PropertyID) VALUES (%s, %s, %s, %s)", (int(row['PriceID']) if not pd.isna(row['PriceID']) else None, row['Price'], float(row['Price_per_SQFT']) if not pd.isna(row['Price_per_SQFT']) else None, int(row['PropertyID']) if not pd.isna(row['PropertyID']) else None))

        conn.commit()

# Database connection details
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
    populate_source_3(conn)
finally:
    conn.close()
