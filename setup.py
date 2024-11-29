import psycopg2
from psycopg2 import sql

def setup_fdw():
    # Connection parameters remain same
    source2_params = {
        "dbname": "real_estate_db_source_2",
        "user": "postgres",
        "password": "admin",
        "host": "localhost",
        "port": "5432"
    }

    source3_params = {
        "dbname": "real_estate_db_source_3",
        "user": "postgres",
        "password": "admin",
        "host": "localhost",
        "port": "5432"
    }

    try:
        conn = psycopg2.connect(**source2_params)
        conn.autocommit = True
        cur = conn.cursor()

        # Same cleanup commands
        cleanup_commands = [
            "DROP SCHEMA IF EXISTS source3 CASCADE;",
            "DROP USER MAPPING IF EXISTS FOR postgres SERVER source3_server;",
            "DROP SERVER IF EXISTS source3_server CASCADE;",
            "DROP EXTENSION IF EXISTS postgres_fdw CASCADE;"
        ]

        for cmd in cleanup_commands:
            cur.execute(cmd)
            print(f"Executed: {cmd}")

        # Same FDW setup
        cur.execute("CREATE EXTENSION postgres_fdw;")
        print("Created postgres_fdw extension")

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

        cur.execute("CREATE SCHEMA source3;")
        print("Created schema source3")

        # Same foreign tables definition
        foreign_tables = {
            "location": """(
                locationid integer,
                location varchar(255)
            )""",
            "properties": """(
                propertyid integer,
                name varchar(255),
                title varchar(255),
                description text,
                locationid integer,
                total_area numeric
            )""",
            "pricing": """(
                priceid integer,
                price text,
                price_per_sqft numeric,
                propertyid integer
            )""",
            "features": """(
                featureid integer,
                baths integer,
                balcony boolean,
                propertyid integer
            )"""
        }

        for table_name, columns in foreign_tables.items():
            create_foreign_table = f"""
            CREATE FOREIGN TABLE source3.{table_name} 
            {columns}
            SERVER source3_server
            OPTIONS (schema_name 'public', table_name '{table_name}');
            """
            cur.execute(create_foreign_table)
            print(f"Created foreign table: source3.{table_name}")

            # Fixed verification query
            cur.execute("""
                SELECT c.relname AS table_name
                FROM pg_class c
                JOIN pg_foreign_table f ON f.ftrelid = c.oid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'source3';
            """)
            foreign_tables = cur.fetchall()
            print("\nVerification:")
            print(f"Number of foreign tables created: {len(foreign_tables)}")
            print("Foreign tables:", [ft[0] for ft in foreign_tables])

            # Additional verification - test connections
            for table_name in foreign_tables:
                try:
                    cur.execute(f"SELECT count(*) FROM source3.{table_name[0]}")
                    count = cur.fetchone()[0]
                    print(f"Table source3.{table_name[0]} is accessible, contains {count} rows")
                except Exception as e:
                    print(f"Warning: Could not access table source3.{table_name[0]}: {str(e)}")

    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    setup_fdw()