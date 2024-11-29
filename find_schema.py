import psycopg2

def get_tables_and_columns(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position;
        """)
        
        tables = {}
        for table_name, column_name in cur.fetchall():
            if table_name not in tables:
                tables[table_name] = []
            tables[table_name].append(column_name)
        
        return tables

def main():
    conn = psycopg2.connect(
        dbname="real_estate_db_source_2",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

    try:
        tables = get_tables_and_columns(conn)
        for table_name, columns in tables.items():
            print(f"Table: {table_name}")
            for column in columns:
                print(f"  - {column}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
    
# Property_Name
# Property_Title
# Property_Type
# Price
# Total_Area
# City
# Location
# Price_Per_Square_Feet
# Description
# Number_Of_Rooms
# Number_Of_Balconies