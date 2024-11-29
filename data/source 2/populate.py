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

def populate_source_2(conn, base_dir):
    # Read data from CSV files
    cities_df = pd.read_csv(base_dir / 'normalized/cities.csv')
    locations_df = pd.read_csv(base_dir / 'normalized/locations.csv')
    property_types_df = pd.read_csv(base_dir / 'normalized/property_types.csv')
    rooms_df = pd.read_csv(base_dir / 'normalized/rooms.csv')
    properties_df = pd.read_csv(base_dir / 'normalized/properties.csv')

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
                row['Property Title'],  # Changed from Property_Title to match CSV column name
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
    base_dir = Path("data/source 2")
    populate_source_2(conn, base_dir)
finally:
    conn.close()
    
# find all nulls

# SELECT 
#     COUNT(*) AS total_records,
#     COUNT(CASE WHEN city IS NULL THEN 1 END) AS null_city,
#     COUNT(CASE WHEN city_id IS NULL THEN 1 END) AS null_city_id
# FROM public.cities;

# SELECT 
#     COUNT(*) AS total_records,
#     COUNT(CASE WHEN location_id IS NULL THEN 1 END) AS null_location_id,
#     COUNT(CASE WHEN Location IS NULL THEN 1 END) AS null_location,
#     COUNT(CASE WHEN city_id IS NULL THEN 1 END) AS null_city_id
# FROM public.locations;

# SELECT 
#     COUNT(*) AS total_records,
#     COUNT(CASE WHEN property_type_id IS NULL THEN 1 END) AS null_property_type_id,
#     COUNT(CASE WHEN property_type IS NULL THEN 1 END) AS null_property_type
# FROM public.property_types;

# SELECT 
#     COUNT(*) AS total_records,
#     COUNT(CASE WHEN room_config_id IS NULL THEN 1 END) AS null_room_config_id,
#     COUNT(CASE WHEN Total_Rooms IS NULL THEN 1 END) AS null_total_rooms,
#     COUNT(CASE WHEN BHK IS NULL THEN 1 END) AS null_bhk
# FROM public.rooms;

# SELECT 
#     COUNT(*) AS total_records,
#     COUNT(CASE WHEN Property_Name IS NULL THEN 1 END) AS null_property_name,
#     COUNT(CASE WHEN Price IS NULL THEN 1 END) AS null_price,
#     COUNT(CASE WHEN Total_Area_SQFT IS NULL THEN 1 END) AS null_total_area_sqft,
#     COUNT(CASE WHEN Price_per_SQFT IS NULL THEN 1 END) AS null_price_per_sqft,
#     COUNT(CASE WHEN property_type_id IS NULL THEN 1 END) AS null_property_type_id,
#     COUNT(CASE WHEN location_id IS NULL THEN 1 END) AS null_location_id,
#     COUNT(CASE WHEN room_config_id IS NULL THEN 1 END) AS null_room_config_id,
#     COUNT(CASE WHEN Location IS NULL THEN 1 END) AS null_location,
#     COUNT(CASE WHEN Balcony IS NULL THEN 1 END) AS null_balcony
# FROM public.properties;