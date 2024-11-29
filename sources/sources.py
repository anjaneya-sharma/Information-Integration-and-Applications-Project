# sources/sources.py
from .schema_mapper import SchemaMapper
from psycopg2 import sql

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

mapper = SchemaMapper()

def get_source_2_query(conditions):
    return mapper.build_query('source_2', conditions)

def get_source_3_query(conditions):
    return mapper.build_query('source_3', conditions)

QUERY_FUNCTIONS = {
    'source_2': get_source_2_query,
    'source_3': get_source_3_query
}