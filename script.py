import psycopg2
from psycopg2 import sql
from flask import Flask, request, render_template

app = Flask(__name__)

def get_unified_query(query_conditions_source_2, query_conditions_source_3):
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
                            WHEN position('acs' in pr.price) > 0 
                            THEN TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, 'acs.*$', ''))::numeric * 100000
                            ELSE TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, '[^0-9.]', ''))::numeric
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
            unified_query = get_unified_query(query_conditions_source_2, query_conditions_source_3)
            cur.execute(unified_query)
            results = cur.fetchall()
            return results
    finally:
        conn_source_2.close()

@app.route('/', methods=['GET', 'POST'])
def index():
    # Initialize form data
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
        'has_balcony': False
    }
    
    if request.method == 'POST':
        # Update form data with submitted values
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
            'has_balcony': bool(request.form.get('has_balcony', False))
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
                        WHEN pr.price LIKE '%Cr%' THEN (REPLACE(REPLACE(REPLACE(pr.price, '₹', ''), ' Cr', ''), ' ', '')::numeric) * 10000000 
                        WHEN pr.price LIKE '%L%' THEN (REPLACE(REPLACE(REPLACE(pr.price, '₹', ''), ' L', ''), ' ', '')::numeric) * 100000 
                        WHEN pr.price LIKE '%acs%' THEN (REPLACE(REPLACE(REPLACE(pr.price, '₹', ''), ' acs', ''), ' ', '')::numeric) * 100000
                        ELSE REPLACE(REPLACE(pr.price, '₹', ''), ' ', '')::numeric 
                    END, 0) >= {form_data['min_price']}
            """)

        if form_data['max_price']:
            query_conditions_source_2.append(f"COALESCE(p.price, 0) <= {form_data['max_price']}")
            query_conditions_source_3.append(f"""
                COALESCE(
                    CASE 
                        WHEN pr.price LIKE '%Cr%' THEN (REPLACE(REPLACE(REPLACE(pr.price, '₹', ''), ' Cr', ''), ' ', '')::numeric) * 10000000 
                        WHEN pr.price LIKE '%L%' THEN (REPLACE(REPLACE(REPLACE(pr.price, '₹', ''), ' L', ''), ' ', '')::numeric) * 100000 
                        WHEN pr.price LIKE '%acs%' THEN (REPLACE(REPLACE(REPLACE(pr.price, '₹', ''), ' acs', ''), ' ', '')::numeric) * 100000
                        ELSE REPLACE(REPLACE(pr.price, '₹', ''), ' ', '')::numeric 
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
        return render_template('index.html', data=results, form=form_data)
    
    return render_template('index.html', data=[], form=form_data)

if __name__ == "__main__":
    app.run(debug=True)