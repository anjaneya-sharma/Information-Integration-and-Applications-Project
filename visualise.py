import psycopg2
from psycopg2 import sql

def connect_to_db():
    """Connect to the PostgreSQL database and return the connection and cursor."""
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="real_estate_db",
            user="postgres",
            password="admin"
        )
        cursor = conn.cursor()
        return conn, cursor
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None, None

def get_table_names(cursor):
    """Get the names of all tables in the public schema."""
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    tables = cursor.fetchall()
    return [table[0] for table in tables]

def print_table_records(cursor, table_name):
    """Print all records from the specified table."""
    try:
        cursor.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name)))
        records = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        print(f"\nTable: {table_name}")
        print(f"Columns: {colnames}")
        for record in records:
            print(record)
    except Exception as e:
        print(f"Error reading table {table_name}: {e}")

def main():
    conn, cursor = connect_to_db()
    if not conn or not cursor:
        return

    try:
        table_names = get_table_names(cursor)
        for table_name in table_names:
            print_table_records(cursor, table_name)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()