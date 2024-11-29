import psycopg2
from psycopg2 import sql
from flask import Flask, request, render_template
import recordlinkage
import pandas as pd

app = Flask(__name__)

def get_unified_query(conn, query_conditions_source_2, query_conditions_source_3):
    query = sql.SQL("""
        SELECT * FROM (
            (
                SELECT 
                    COALESCE(p.property_name, 'Unknown') AS Property_Name,
                    p.property_title AS Property_Title,
                    COALESCE(pt.property_type, 'Not Specified') AS Property_Type,
                    COALESCE(p.price, 0) AS Price,
                    COALESCE(p.total_area_sqft, 0) AS Total_Area,
                    COALESCE(c.city, 'Unknown') AS City,
                    COALESCE(l.location, 'Unknown') AS Location,
                    COALESCE(p.price_per_sqft, 0) AS Price_per_SQFT,
                    COALESCE(p.description, 'No description available') AS Description,
                    COALESCE(r.total_rooms, 0) AS Number_Of_Rooms,
                    COALESCE(p.balcony, false) AS Number_Of_Balconies,
                    'source_2' as source
                FROM properties p
                LEFT JOIN locations l ON p.location_id = l.location_id
                LEFT JOIN cities c ON l.city_id = c.city_id
                LEFT JOIN property_types pt ON p.property_type_id = pt.property_type_id
                LEFT JOIN rooms r ON p.room_config_id = r.room_config_id
                WHERE {source_2_conditions}
                LIMIT 100
            )
            UNION ALL
            (
                SELECT 
                    COALESCE(p.name, 'Unknown') AS Property_Name,
                    COALESCE(p.title, 'No title') AS Property_Title,
                    'Not Specified' AS Property_Type,
                    COALESCE((
                        CASE 
                            WHEN position('Cr' in pr.price) > 0 
                                THEN TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, 'Cr.*$', ''))::numeric * 10000000
                            WHEN position('L' in pr.price) > 0 
                                THEN TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, 'L.*$', ''))::numeric * 100000
                            WHEN position('k' in pr.price) > 0 
                                THEN TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, 'k.*$', ''))::numeric * 1000
                            ELSE TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, '[^0-9.]', '', 'g'))::numeric
                        END
                    ), 0) AS Price,
                    COALESCE(p.total_area, 0) AS Total_Area,
                    'Unknown' AS City,
                    COALESCE(l.location, 'Unknown') AS Location,
                    COALESCE(pr.price_per_sqft, 0) AS Price_per_SQFT,
                    COALESCE(p.description, 'No description available') AS Description,
                    COALESCE(f.baths, 0) AS Number_Of_Rooms,
                    COALESCE(f.balcony, false) AS Number_Of_Balconies,
                    'source_3' as source
                FROM source3.properties p
                LEFT JOIN source3.location l ON p.locationid = l.locationid
                LEFT JOIN source3.pricing pr ON p.propertyid = pr.propertyid
                LEFT JOIN source3.features f ON p.propertyid = f.propertyid
                WHERE {source_3_conditions}
                LIMIT 100
            )
        ) AS unified_properties
        ORDER BY Price DESC, Total_Area DESC
        LIMIT 200
    """).format(
        source_2_conditions=sql.SQL(query_conditions_source_2),
        source_3_conditions=sql.SQL(query_conditions_source_3)
    )
    print("Generated SQL Query:", query.as_string(conn))
    return query

def query_global_properties(query_conditions_source_2, query_conditions_source_3):
    conn_source_2 = psycopg2.connect(
        dbname="real_estate_db_source_2",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

    try:
        with conn_source_2.cursor() as cur:
            unified_query = get_unified_query(conn_source_2, query_conditions_source_2, query_conditions_source_3)
            print("Executing Query...")
            cur.execute(unified_query)
            results = cur.fetchall()
            print("Query Results:", results)
            return results
    except Exception as e:
        print("Error executing query:", e)
        return []  # Return an empty list in case of an error
    finally:
        conn_source_2.close()

def remove_duplicates(results):
    df = pd.DataFrame(results, columns=[
        'Property_Name', 'Property_Title', 'Property_Type', 'Price',
        'Total_Area', 'City', 'Location', 'Price_per_SQFT',
        'Description', 'Number_Of_Rooms', 'Number_Of_Balconies', 'Source'
    ])
    
    print(f"Original records: {len(df)}")  # Logging original count
    
    # Create a temporary copy for matching
    df_temp = df.copy()
    
    # Preprocess data in the temporary copy
    df_temp['Property_Name'] = df_temp['Property_Name'].str.lower().str.strip()
    df_temp['Location'] = df_temp['Location'].str.lower().str.strip()
    
    # Indexing
    indexer = recordlinkage.Index()
    indexer.block(['Property_Name', 'Location'])
    candidate_links = indexer.index(df_temp)
    
    # Comparison
    compare = recordlinkage.Compare()
    compare.string('Property_Name', 'Property_Name', method='jarowinkler', threshold=0.85, label='name')
    compare.string('Location', 'Location', method='jarowinkler', threshold=0.85, label='location')
    features = compare.compute(candidate_links, df_temp)
    
    # Find duplicates
    duplicates = features[features.sum(axis=1) > 1].index
    duplicate_indices = duplicates.get_level_values(1)
    
    print(f"Duplicate records to remove: {len(duplicate_indices)}")  # Logging duplicates
    
    # Drop duplicates from the original DataFrame
    df_unique = df.drop(duplicate_indices).values.tolist()
    print(f"Unique records after removal: {len(df_unique)}")  # Logging unique count
    return df_unique

@app.route('/', methods=['GET', 'POST'])
def index():
    form_data = {
        'property_name': '',
        'city': '',
        'location': '',
        'min_price': '',
        'max_price': '',
        'min_area': '',
        'max_area': '',
        'property_type': '',
        'min_rooms': '',
        'has_balcony': False,
        'hide_duplicates': False
    }
    
    if request.method == 'POST':
        form_data.update({
            'property_name': request.form.get('property_name', ''),
            'city': request.form.get('city', ''),
            'location': request.form.get('location', ''),
            'min_price': request.form.get('min_price', ''),
            'max_price': request.form.get('max_price', ''),
            'min_area': request.form.get('min_area', ''),
            'max_area': request.form.get('max_area', ''),
            'property_type': request.form.get('property_type', ''),
            'min_rooms': request.form.get('min_rooms', ''),
            'has_balcony': bool(request.form.get('has_balcony', False)),
            'hide_duplicates': bool(request.form.get('hide_duplicates', False))
        })
        
        query_conditions_source_2 = []
        query_conditions_source_3 = []
        
        if form_data['property_name']:
            query_conditions_source_2.append(f"COALESCE(p.property_name, '') ILIKE '%{form_data['property_name']}%'")
            query_conditions_source_3.append(f"COALESCE(p.name, '') ILIKE '%{form_data['property_name']}%'")
        if form_data['city']:
            query_conditions_source_2.append(f"COALESCE(c.city, '') ILIKE '%{form_data['city']}%'")
            query_conditions_source_3.append("1=0")
        if form_data['location']:
            query_conditions_source_2.append(f"COALESCE(l.location, '') ILIKE '%{form_data['location']}%'")
            query_conditions_source_3.append(f"COALESCE(l.location, '') ILIKE '%{form_data['location']}%'")
        if form_data['min_price']:
            query_conditions_source_2.append(f"COALESCE(p.price, 0) >= {form_data['min_price']}")
            query_conditions_source_3.append(f"""
                COALESCE(
                    CASE 
                        WHEN pr.price LIKE '%Cr%' THEN (TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, 'Cr.*$', ''))::numeric) * 10000000 
                        WHEN pr.price LIKE '%L%' THEN (TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, 'L.*$', ''))::numeric) * 100000 
                        WHEN pr.price LIKE '%k%' THEN (TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, 'k.*$', ''))::numeric) * 1000
                        ELSE TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, '[^0-9.]', '', 'g'))::numeric 
                    END, 0) >= {form_data['min_price']}
            """)
        if form_data['max_price']:
            query_conditions_source_2.append(f"COALESCE(p.price, 0) <= {form_data['max_price']}")
            query_conditions_source_3.append(f"""
                COALESCE(
                    CASE 
                        WHEN pr.price LIKE '%Cr%' THEN (TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, 'Cr.*$', ''))::numeric) * 10000000 
                        WHEN pr.price LIKE '%L%' THEN (TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, 'L.*$', ''))::numeric) * 100000 
                        WHEN pr.price LIKE '%k%' THEN (TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, 'k.*$', ''))::numeric) * 1000
                        ELSE TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, '[^0-9.]', '', 'g'))::numeric 
                    END, 0) <= {form_data['max_price']}
            """)
        if form_data['min_area']:
            query_conditions_source_2.append(f"COALESCE(p.total_area_sqft, 0) >= {form_data['min_area']}")
            query_conditions_source_3.append(f"COALESCE(p.total_area, 0) >= {form_data['min_area']}")
        if form_data['max_area']:
            query_conditions_source_2.append(f"COALESCE(p.total_area_sqft, 0) <= {form_data['max_area']}")
            query_conditions_source_3.append(f"COALESCE(p.total_area, 0) <= {form_data['max_area']}")
        if form_data['property_type']:
            query_conditions_source_2.append(f"COALESCE(pt.property_type, '') ILIKE '%{form_data['property_type']}%'")
            query_conditions_source_3.append("1=0")  # Source 3 doesn't have property type
        if form_data['min_rooms']:
            query_conditions_source_2.append(f"COALESCE(r.total_rooms, 0) >= {form_data['min_rooms']}")
            query_conditions_source_3.append("1=0")  # Source 3 doesn't have rooms info
        if form_data['has_balcony']:
            query_conditions_source_2.append("COALESCE(p.balcony, false) = true")
            query_conditions_source_3.append("COALESCE(f.balcony, false) = true")
        
        query_conditions_source_2 = " AND ".join(query_conditions_source_2) if query_conditions_source_2 else "1=1"
        query_conditions_source_3 = " AND ".join(query_conditions_source_3) if query_conditions_source_3 else "1=1"
        
        results = query_global_properties(query_conditions_source_2, query_conditions_source_3)
        
        if form_data['hide_duplicates']:
            results = remove_duplicates(results)
        
        # Convert Price and Total Area to integers
        processed_results = []
        for row in results:
            row = list(row)
            try:
                row[3] = int(row[3])  # Price
            except (ValueError, TypeError):
                row[3] = 0
            try:
                row[4] = int(row[4])  # Total Area
            except (ValueError, TypeError):
                row[4] = 0
            processed_results.append(tuple(row))
        
        return render_template('index.html', data=processed_results, form=form_data)
    
    return render_template('index.html', data=[], form=form_data)

if __name__ == "__main__":
    app.run(debug=True)