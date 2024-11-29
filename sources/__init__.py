# sources/__init__.py
from .sources import CONNECTION_PARAMS, QUERY_FUNCTIONS, mapper
from psycopg2 import sql

# Connection parameters for each source
CONNECTION_PARAMS = {
    'source_2': {
        'dbname': 'real_estate_db_source_2',
        'user': 'postgres',
        'password': 'admin',
        'host': 'localhost',
        'port': '5432'
    },
    'source_3': {
        'dbname': 'real_estate_db_source_3',
        'user': 'postgres',
        'password': 'admin',
        'host': 'localhost',
        'port': '5432'
    }
}

# Query functions for each source
def get_source_2_query(conditions):
    query = f"""
        SELECT 
            COALESCE(p.{mapper.get_column('source_2', 'Property_Name')}, 'Unknown') AS Property_Name,
            p.{mapper.get_column('source_2', 'Property_Title')} AS Property_Title,
            COALESCE(pt.property_type, 'Not Specified') AS Property_Type,
            COALESCE(p.{mapper.get_column('source_2', 'Price')}, 0) AS Price,
            COALESCE(p.{mapper.get_column('source_2', 'Total_Area')}, 0) AS Total_Area,
            COALESCE(c.city, 'Unknown') AS City,
            COALESCE(l.location, 'Unknown') AS Location,
            COALESCE(p.{mapper.get_column('source_2', 'Price_per_SQFT')}, 0) AS Price_per_SQFT,
            COALESCE(p.description, 'No description available') AS Description,
            COALESCE(r.total_rooms, 0) AS Number_Of_Rooms,
            COALESCE(p.{mapper.get_column('source_2', 'Number_Of_Balconies')}, false) AS Number_Of_Balconies,
            'source_2' as source
        FROM properties p
        LEFT JOIN property_types pt ON p.property_type_id = pt.property_type_id
        LEFT JOIN locations l ON p.location_id = l.location_id
        LEFT JOIN cities c ON l.city_id = c.city_id
        LEFT JOIN rooms r ON p.room_config_id = r.room_config_id
        WHERE {conditions}
    """
    return query

def query_source_3(conditions):
    query = sql.SQL("""
        SELECT 
            COALESCE(p.name, 'Unknown') AS Property_Name,
            COALESCE(p.title, 'No title') AS Property_Title,
            'Not Specified' AS Property_Type,
            COALESCE(
                CASE 
                    WHEN position('Cr' in pr.price) > 0 
                        THEN TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, 'Cr.*$', ''))::numeric * 10000000
                    WHEN position('L' in pr.price) > 0 
                        THEN TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, 'L.*$', ''))::numeric * 100000
                    WHEN position('k' in pr.price) > 0 
                        THEN TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, 'k.*$', ''))::numeric * 1000
                    ELSE TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, '[^0-9.]', '', 'g'))::numeric
                END, 0) AS Price,
            COALESCE(p.total_area, 0) AS Total_Area,
            'Unknown' AS City,
            COALESCE(l.location, 'Unknown') AS Location,
            COALESCE(pr.price_per_sqft, 0) AS Price_per_SQFT,
            COALESCE(p.description, 'No description available') AS Description,
            COALESCE(f.baths, 0) AS Number_Of_Rooms,
            COALESCE(f.balcony, false) AS Number_Of_Balconies,
            'source_3' as source
        FROM properties p
        LEFT JOIN location l ON p.locationid = l.locationid
        LEFT JOIN pricing pr ON p.propertyid = pr.propertyid
        LEFT JOIN features f ON p.propertyid = f.propertyid
        WHERE {conditions}
        LIMIT 100
    """).format(conditions=sql.SQL(conditions))
    return query

# Mapping of source names to query functions
QUERY_FUNCTIONS = {
    'source_2': get_source_2_query,
    'source_3': query_source_3
}