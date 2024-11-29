import psycopg2
from psycopg2 import sql
from flask import Flask, request, render_template
import requests
import recordlinkage
import pandas as pd
from sources import CONNECTION_PARAMS, QUERY_FUNCTIONS

app = Flask(__name__)


import pandas as pd
import recordlinkage

def remove_duplicates(results):
    # Convert the combined data to a pandas DataFrame
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

    
# Function to fetch data from System A (Source 2)
def fetch_data_from_source_2(query_conditions):
    try:
        # Create the URL with query conditions to fetch from System A
        source_2_api_url = "http://192.168.0.100:5000/get_properties"  # Replace <system_a_ip> with System A's IP
        response = requests.get(source_2_api_url, params={'conditions': query_conditions}, timeout=(10,60))
        if response.status_code == 200:
            print("Data received from System A:", response.json())
        else:
            print(f"Failed to receive data from System A: {response.status_code} {response.text}")

        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from source 2:", e)
        return []

# Function to query global properties from source 3 (local to System B)
def query_source_3(query_conditions):
    results = []
    try:
        conn_params = CONNECTION_PARAMS.get('source_3')
        conn = psycopg2.connect(**conn_params)
        query_func = QUERY_FUNCTIONS.get('source_3')
        if query_func:
            query = query_func(query_conditions)
            with conn.cursor() as cur:
                cur.execute(query)
                results += cur.fetchall()
        conn.close()
    except Exception as e:
        print(f"Error executing query for source 3:", e)
    return results

@app.route('/', methods=['GET', 'POST'])
def index():
    print("\napi is running\n")
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

        query_conditions = "1=1"  # Build appropriate conditions string from the form data

        # Step 1: Fetch data from System A
        data_source_2 = fetch_data_from_source_2(query_conditions)

        print(data_source_2)

        print("\n\n\n")

        # Step 2: Query data from System B (Source 3)
        data_source_3 = query_source_3(query_conditions)

        # print(data_source_3)

        # print("\n\n\n")

        # Step 3: Combine the data from both sources
        combined_data = data_source_2 + data_source_3

        # Step 4: Remove duplicates if requested
        if form_data['hide_duplicates']:
            combined_data = remove_duplicates(combined_data)

        return render_template('index.html', data=combined_data, form=form_data)

    return render_template('index.html', data=[], form=form_data)

if __name__ == "__main__":
    app.run(debug=True)
