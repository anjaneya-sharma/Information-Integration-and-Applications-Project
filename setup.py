import psycopg2
from psycopg2 import sql

def setup_fdw():
    # Connection parameters
    source2_params = {
        "dbname": "real_estate_db_source_2",
        "user": "postgres",
        "password": "admin",
        "host": "localhost",
        "port": "5432"
    }

    # Remote server (source3) parameters
    source3_params = {
        "dbname": "real_estate_db_source_3",
        "user": "postgres",
        "password": "admin",
        "host": "localhost",
        "port": "5432"
    }

    try:
        # Connect to source2 database
        conn = psycopg2.connect(**source2_params)
        conn.autocommit = True
        cur = conn.cursor()

        # Drop existing objects
        cleanup_commands = [
            "DROP FOREIGN TABLE IF EXISTS source3_properties CASCADE;",
            "DROP FOREIGN TABLE IF EXISTS source3_location CASCADE;",
            "DROP FOREIGN TABLE IF EXISTS source3_pricing CASCADE;",
            "DROP FOREIGN TABLE IF EXISTS source3_features CASCADE;",
            "DROP USER MAPPING IF EXISTS FOR postgres SERVER source3_server;",
            "DROP SERVER IF EXISTS source3_server CASCADE;",
            "DROP EXTENSION IF EXISTS postgres_fdw CASCADE;"
        ]

        for cmd in cleanup_commands:
            cur.execute(cmd)
            print(f"Executed: {cmd}")

        # Create FDW extension
        cur.execute("CREATE EXTENSION postgres_fdw;")
        print("Created postgres_fdw extension")

        # Create server
        create_server = f"""
        CREATE SERVER source3_server
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (
            host '{source3_params['host']}',
            dbname '{source3_params['dbname']}',
            port '{source3_params['port']}'
        );
        """
        cur.execute(create_server)
        print("Created foreign server")

        # Create user mapping
        create_mapping = f"""
        CREATE USER MAPPING FOR postgres
        SERVER source3_server
        OPTIONS (
            user '{source3_params['user']}',
            password '{source3_params['password']}'
        );
        """
        cur.execute(create_mapping)
        print("Created user mapping")

        # Create foreign tables
        foreign_tables = {
            "source3_properties": """(
                propertyid integer,
                name varchar(255),
                title varchar(255),
                description text,
                locationid integer,
                total_area numeric
            )""",
            "source3_location": """(
                locationid integer,
                location varchar(255)
            )""",
            "source3_pricing": """(
                priceid integer,
                price text,
                price_per_sqft numeric,
                propertyid integer
            )""",
            "source3_features": """(
                featureid integer,
                baths integer,
                balcony boolean,
                propertyid integer
            )"""
        }

        for table_name, columns in foreign_tables.items():
            create_foreign_table = f"""
            CREATE FOREIGN TABLE {table_name} 
            {columns}
            SERVER source3_server
            OPTIONS (schema_name 'public', table_name '{table_name.replace("source3_", "")}');
            """
            cur.execute(create_foreign_table)
            print(f"Created foreign table: {table_name}")

        # Verify setup
        cur.execute("SELECT * FROM pg_foreign_table;")
        foreign_tables = cur.fetchall()
        print("\nVerification:")
        print(f"Number of foreign tables created: {len(foreign_tables)}")

    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    setup_fdw()