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
            CREATE TABLE cities (
                city VARCHAR(255),
                city_id SERIAL PRIMARY KEY
            );
        """)
        cur.execute("""
            CREATE TABLE locations (
                location_id SERIAL PRIMARY KEY,
                Location VARCHAR(255),
                city_id INT,
                FOREIGN KEY (city_id) REFERENCES cities(city_id)
            );
        """)
        cur.execute("""
            CREATE TABLE property_types (
                property_type_id SERIAL PRIMARY KEY,
                property_type VARCHAR(255)
            );
        """)
        cur.execute("""
            CREATE TABLE rooms (
                room_config_id SERIAL PRIMARY KEY,
                Total_Rooms INT,
                BHK INT
            );
        """)
        cur.execute("""
            CREATE TABLE properties (
                property_id SERIAL PRIMARY KEY,
                Property_Name VARCHAR(255),
                Property_Title VARCHAR(255),
                Price NUMERIC,
                Total_Area_SQFT NUMERIC,
                Price_per_SQFT NUMERIC,
                property_type_id INT,
                location_id INT,
                room_config_id INT,
                Location VARCHAR(255),
                Description TEXT,
                Balcony BOOLEAN,
                FOREIGN KEY (property_type_id) REFERENCES property_types(property_type_id),
                FOREIGN KEY (location_id) REFERENCES locations(location_id),
                FOREIGN KEY (room_config_id) REFERENCES rooms(room_config_id)
            );
        """)
        conn.commit()

# Populate Data Function
def populate_source_2(conn):
    # Using get_file_path to construct paths dynamically and validate their existence
    cities_path = get_file_path('normalized', 'cities.csv')
    locations_path = get_file_path( 'normalized', 'locations.csv')
    property_types_path = get_file_path('normalized', 'property_types.csv')
    rooms_path = get_file_path('normalized', 'rooms.csv')
    properties_path = get_file_path('normalized', 'properties.csv')

    # Load data from CSV files
    cities_df = pd.read_csv(cities_path)
    locations_df = pd.read_csv(locations_path)
    property_types_df = pd.read_csv(property_types_path)
    rooms_df = pd.read_csv(rooms_path)
    properties_df = pd.read_csv(properties_path)

    with conn.cursor() as cur:
        for _, row in cities_df.iterrows():
            cur.execute("INSERT INTO cities (city, city_id) VALUES (%s, %s)", 
                      (row['city'], int(row['city_id']) if not pd.isna(row['city_id']) else None))

        for _, row in locations_df.iterrows():
            cur.execute("INSERT INTO locations (location_id, Location, city_id) VALUES (%s, %s, %s)", 
                      (int(row['location_id']) if not pd.isna(row['location_id']) else None, 
                       row['Location'], 
                       int(row['city_id']) if not pd.isna(row['city_id']) else None))

        for _, row in property_types_df.iterrows():
            cur.execute("INSERT INTO property_types (property_type_id, property_type) VALUES (%s, %s)", 
                      (int(row['property_type_id']) if not pd.isna(row['property_type_id']) else None, 
                       row['property_type']))

        for _, row in rooms_df.iterrows():
            cur.execute("INSERT INTO rooms (room_config_id, Total_Rooms, BHK) VALUES (%s, %s, %s)", 
                      (int(row['room_config_id']) if not pd.isna(row['room_config_id']) else None,
                       int(row['Total_Rooms']) if not pd.isna(row['Total_Rooms']) else None,
                       int(row['BHK']) if not pd.isna(row['BHK']) else None))

        for _, row in properties_df.iterrows():
            cur.execute("""
                INSERT INTO properties (
                    property_id, Property_Name, Property_Title, Price, Total_Area_SQFT, 
                    Price_per_SQFT, property_type_id, location_id, room_config_id, 
                    Location, Description, Balcony
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                int(row['property_id']) if not pd.isna(row['property_id']) else None,
                row['Property_Name'],
                row['Property Title'],
                float(row['Price']) if not pd.isna(row['Price']) else None,
                float(row['Total_Area(SQFT)']) if not pd.isna(row['Total_Area(SQFT)']) else None,
                float(row['Price_per_SQFT']) if not pd.isna(row['Price_per_SQFT']) else None,
                int(row['property_type_id']) if not pd.isna(row['property_type_id']) else None,
                int(row['location_id']) if not pd.isna(row['location_id']) else None,
                int(row['room_config_id']) if not pd.isna(row['room_config_id']) else None,
                row['Location'],
                row.get('Description', None),
                bool(row['Balcony']) if not pd.isna(row['Balcony']) else None
            ))
        conn.commit()

# Database connection details
dbname = "real_estate_db_source_2"
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
    populate_source_2(conn)
finally:
    conn.close()
