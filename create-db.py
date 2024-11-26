import psycopg2
from psycopg2 import Error
import time

def check_db_exists(cursor):
    cursor.execute("SELECT 1 FROM pg_database WHERE datname='real_estate_db'")
    return cursor.fetchone() is not None

def create_database():
    conn = None
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="admin"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        if check_db_exists(cursor):
            print("WARNING: Database 'real_estate_db' already exists!")
            print("Press Enter 5 times to confirm database deletion...")
            for i in range(5, 0, -1):
                input(f"Press Enter to confirm ({i} remaining)...")
            print("Dropping existing database...")
            
        cursor.execute("DROP DATABASE IF EXISTS real_estate_db")
        cursor.execute("CREATE DATABASE real_estate_db")
        print("Database created successfully")
        
    except (Exception, Error) as error:
        print("Error while creating database:", error)
    finally:
        if conn:
            cursor.close()
            conn.close()

def create_schema():
    conn = None
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="real_estate_db",
            user="postgres",
            password="admin"
        )
        cursor = conn.cursor()
        
        with open('schema.sql', 'r') as schema_file:
            schema_sql = schema_file.read()
            cursor.execute(schema_sql)
        
        conn.commit()
        print("Schema created successfully")
        
    except (Exception, Error) as error:
        print("Error while creating schema:", error)
    finally:
        if conn:
            cursor.close()
            conn.close()

create_database()
create_schema()