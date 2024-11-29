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

def build_query(source, conditions):
    """Build standardized query for any source"""
    if source == 'source_2':
        return f"""
            SELECT 
                COALESCE(p.{mapper.get_column(source, 'Property_Name')}, 'Unknown') AS Property_Name,
                COALESCE(p.{mapper.get_column(source, 'Property_Title')}, '') AS Property_Title,
                COALESCE(pt.{mapper.get_column(source, 'Property_Type')}, 'Not Specified') AS Property_Type,
                COALESCE(p.{mapper.get_column(source, 'Price')}, 0) AS Price,
                COALESCE(p.{mapper.get_column(source, 'Total_Area')}, 0) AS Total_Area,
                COALESCE(c.{mapper.get_column(source, 'City')}, 'Unknown') AS City,
                COALESCE(l.{mapper.get_column(source, 'Location')}, 'Unknown') AS Location,
                COALESCE(p.{mapper.get_column(source, 'Price_per_SQFT')}, 0) AS Price_per_SQFT,
                COALESCE(p.{mapper.get_column(source, 'Description')}, 'No description') AS Description,
                COALESCE(r.{mapper.get_column(source, 'Number_Of_Rooms')}, 0) AS Number_Of_Rooms,
                COALESCE(p.{mapper.get_column(source, 'Number_Of_Balconies')}, false) AS Number_Of_Balconies,
                '{source}' as source
            FROM properties p
            LEFT JOIN property_types pt ON p.{mapper.get_column(source, 'property_type_id')} = pt.{mapper.get_column(source, 'property_type_id')}
            LEFT JOIN locations l ON p.{mapper.get_column(source, 'location_id')} = l.{mapper.get_column(source, 'location_id')}
            LEFT JOIN cities c ON l.{mapper.get_column(source, 'city_id')} = c.{mapper.get_column(source, 'city_id')}
            LEFT JOIN rooms r ON p.{mapper.get_column(source, 'room_config_id')} = r.{mapper.get_column(source, 'room_config_id')}
            WHERE {conditions}
        """
    else:  # source_3
        return f"""
            SELECT 
                COALESCE(p.{mapper.get_column(source, 'Property_Name')}, 'Unknown') AS Property_Name,
                COALESCE(p.{mapper.get_column(source, 'Property_Title')}, '') AS Property_Title,
                'Not Specified' AS Property_Type,
                COALESCE(
                    CASE 
                        WHEN position('Cr' in pr.{mapper.get_column(source, 'Price')}) > 0 
                            THEN TRIM(BOTH '₹ ' FROM regexp_replace(pr.{mapper.get_column(source, 'Price')}, 'Cr.*$', ''))::numeric * 10000000
                        WHEN position('L' in pr.{mapper.get_column(source, 'Price')}) > 0 
                            THEN TRIM(BOTH '₹ ' FROM regexp_replace(pr.{mapper.get_column(source, 'Price')}, 'L.*$', ''))::numeric * 100000
                        WHEN position('k' in pr.{mapper.get_column(source, 'Price')}) > 0 
                            THEN TRIM(BOTH '₹ ' FROM regexp_replace(pr.{mapper.get_column(source, 'Price')}, 'k.*$', ''))::numeric * 1000
                        ELSE TRIM(BOTH '₹ ' FROM regexp_replace(pr.{mapper.get_column(source, 'Price')}, '[^0-9.]', '', 'g'))::numeric
                    END, 0) AS Price,
                COALESCE(p.{mapper.get_column(source, 'Total_Area')}, 0) AS Total_Area,
                'Unknown' AS City,
                COALESCE(l.{mapper.get_column(source, 'Location')}, 'Unknown') AS Location,
                COALESCE(pr.{mapper.get_column(source, 'Price_per_SQFT')}, 0) AS Price_per_SQFT,
                COALESCE(p.{mapper.get_column(source, 'Description')}, 'No description') AS Description,
                COALESCE(f.{mapper.get_column(source, 'Number_Of_Rooms')}, 0) AS Number_Of_Rooms,
                COALESCE(f.{mapper.get_column(source, 'Number_Of_Balconies')}, false) AS Number_Of_Balconies,
                '{source}' as source
            FROM properties p
            LEFT JOIN location l ON p.{mapper.get_column(source, 'locationid')} = l.{mapper.get_column(source, 'locationid')}
            LEFT JOIN pricing pr ON p.{mapper.get_column(source, 'propertyid')} = pr.{mapper.get_column(source, 'propertyid')}
            LEFT JOIN features f ON p.{mapper.get_column(source, 'propertyid')} = f.{mapper.get_column(source, 'propertyid')}
            WHERE {conditions}
            LIMIT 100
        """

def get_source_2_query(conditions):
    return build_query('source_2', conditions)

def get_source_3_query(conditions):
    return build_query('source_3', conditions)

QUERY_FUNCTIONS = {
    'source_2': get_source_2_query,
    'source_3': get_source_3_query
}

# Mapping of source names to query functions
QUERY_FUNCTIONS = {
    'source_2': get_source_2_query,
    'source_3': get_source_3_query
}