import psycopg2
from psycopg2 import sql
from flask import Flask, request, render_template
import recordlinkage
import pandas as pd
from sources import CONNECTION_PARAMS, QUERY_FUNCTIONS, mapper

app = Flask(__name__)

def query_global_properties(query_conditions):
    results = []
    for source_name, conn_params in CONNECTION_PARAMS.items():
        tries = 0
        max_tries = 2
        conn = None

        while tries < max_tries:
            try:
                if conn:
                    conn.close()  # Close any existing connection
                conn = psycopg2.connect(**conn_params)
                
                query_func = QUERY_FUNCTIONS.get(source_name)
                if query_func:
                    query = query_func(query_conditions.get(source_name, ""))
                    print(f"Executing query: {query}")
                    with conn.cursor() as cur:
                        cur.execute(query)
                        results += cur.fetchall()
                        conn.commit()  # Commit successful transaction
                    break
                
            except psycopg2.Error as e:
                tries += 1
                if conn:
                    conn.rollback()  # Rollback failed transaction
                
                error_msg = str(e)
                if "column" in error_msg.lower() and "does not exist" in error_msg.lower():
                    print("\n=== Column Error Handling ===")
                    print(f"Error message: {error_msg}")
                    
                    # Extract failed column name
                    error_line = error_msg.split('\n')[0]
                    failed_column = error_line.split('column ')[1].split(' ')[0].replace('p.', '')
                    print(f"Failed column: {failed_column}")
                    
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_schema = 'public' 
                            AND table_name = 'properties'
                        """)
                        actual_columns = [row[0] for row in cur.fetchall()]
                        conn.commit()  # Commit schema query
                    
                    print(f"Available columns: {actual_columns}")
                    
                    # Use mapper to find matching column
                    mapped_column = mapper.find_matching_column(source_name, failed_column, actual_columns)
                    print(f"Mapper returned: {mapped_column}")
                    
                    if mapped_column and mapped_column != failed_column:
                        print(f"Updating query with new mapping: {failed_column} -> {mapped_column}")
                        # Update schema store
                        if source_name not in mapper.schema:
                            mapper.schema[source_name] = {'columns': {}}
                        for std_name, curr_col in mapper.schema[source_name]['columns'].items():
                            if curr_col == failed_column:
                                mapper.schema[source_name]['columns'][std_name] = mapped_column
                                print(f"Updated schema mapping for {std_name}")
                        mapper._save_schema()
                        
                        # Update query condition
                        query_conditions[source_name] = query_conditions[source_name].replace(
                            f"p.{failed_column}", 
                            f"p.{mapped_column}"
                        )
                        continue
                
                print(f"Error executing query for {source_name}:", e)
                break
                
            except Exception as e:
                if conn:
                    conn.rollback()  # Rollback on any other error
                print(f"Unexpected error for {source_name}:", e)
                break
                
            finally:
                if conn:
                    conn.close()
                    
    return results

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
        
        query_conditions = {source: [] for source in CONNECTION_PARAMS.keys()}
        
        # script.py
        if form_data['property_name']:
            # Get mapped column names for each source
            query_conditions['source_2'].append(
                f"COALESCE(p.{mapper.get_column('source_2', 'Property_Name')}, '') ILIKE '%{form_data['property_name']}%'"
            )
            query_conditions['source_3'].append(
                f"COALESCE(p.{mapper.get_column('source_3', 'Property_Name')}, '') ILIKE '%{form_data['property_name']}%'"
            )
            query_conditions['source_3'].append(f"COALESCE(p.name, '') ILIKE '%{form_data['property_name']}%'")
        if form_data['city']:
            query_conditions['source_2'].append(f"COALESCE(c.city, '') ILIKE '%{form_data['city']}%'")
            query_conditions['source_3'].append("1=0")  # Source 3 doesn't have city info
        if form_data['location']:
            query_conditions['source_2'].append(f"COALESCE(l.location, '') ILIKE '%{form_data['location']}%'")
            query_conditions['source_3'].append(f"COALESCE(l.location, '') ILIKE '%{form_data['location']}%'")
        if form_data['min_price']:
            query_conditions['source_2'].append(f"COALESCE(p.price, 0) >= {form_data['min_price']}")
            query_conditions['source_3'].append(f"""
                COALESCE(
                    CASE 
                        WHEN pr.price LIKE '%Cr%' THEN (TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, 'Cr.*$', ''))::numeric) * 10000000 
                        WHEN pr.price LIKE '%L%' THEN (TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, 'L.*$', ''))::numeric) * 100000 
                        WHEN pr.price LIKE '%k%' THEN (TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, 'k.*$', ''))::numeric) * 1000
                        ELSE TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, '[^0-9.]', '', 'g'))::numeric 
                    END, 0) >= {form_data['min_price']}
            """)
        if form_data['max_price']:
            query_conditions['source_2'].append(f"COALESCE(p.price, 0) <= {form_data['max_price']}")
            query_conditions['source_3'].append(f"""
                COALESCE(
                    CASE 
                        WHEN pr.price LIKE '%Cr%' THEN (TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, 'Cr.*$', ''))::numeric) * 10000000 
                        WHEN pr.price LIKE '%L%' THEN (TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, 'L.*$', ''))::numeric) * 100000 
                        WHEN pr.price LIKE '%k%' THEN (TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, 'k.*$', ''))::numeric) * 1000
                        ELSE TRIM(BOTH '₹ ' FROM regexp_replace(pr.price, '[^0-9.]', '', 'g'))::numeric 
                    END, 0) <= {form_data['max_price']}
            """)
        if form_data['min_area']:
            query_conditions['source_2'].append(f"COALESCE(p.total_area_sqft, 0) >= {form_data['min_area']}")
            query_conditions['source_3'].append(f"COALESCE(p.total_area, 0) >= {form_data['min_area']}")
        if form_data['max_area']:
            query_conditions['source_2'].append(f"COALESCE(p.total_area_sqft, 0) <= {form_data['max_area']}")
            query_conditions['source_3'].append(f"COALESCE(p.total_area, 0) <= {form_data['max_area']}")
        if form_data['property_type']:
            query_conditions['source_2'].append(f"COALESCE(pt.property_type, '') ILIKE '%{form_data['property_type']}%'")
            query_conditions['source_3'].append("1=0")  # Source 3 doesn't have property type
        if form_data['min_rooms']:
            query_conditions['source_2'].append(f"COALESCE(r.total_rooms, 0) >= {form_data['min_rooms']}")
            query_conditions['source_3'].append(f"COALESCE(f.baths, 0) >= {form_data['min_rooms']}")  # Use f.baths for source 3
        if form_data['has_balcony']:
            query_conditions['source_2'].append("COALESCE(p.balcony, false) = true")
            query_conditions['source_3'].append("COALESCE(f.balcony, false) = true")
        
        query_conditions = {k: " AND ".join(v) if v else "1=1" for k, v in query_conditions.items()}
        
        results = query_global_properties(query_conditions)
        
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