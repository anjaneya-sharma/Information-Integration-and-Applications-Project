import pandas as pd
import numpy as np
from pathlib import Path

def create_directory_structure():
    """Create necessary directories if they don't exist"""
    Path("data/source 2/normalized").mkdir(parents=True, exist_ok=True)

def normalize_real_estate_data(input_file_path):
    # Read data
    df = pd.read_csv(input_file_path)
    print(f"Initial data rows: {len(df)}")
    
    # Handle Balcony as categorical Yes/No
    df['Balcony'] = df['Balcony'].map({'Yes': 1, 'No': 0}).fillna(0)
    print(f"Balcony values count:\n{df['Balcony'].value_counts()}")
    
    # Convert other numeric columns
    numeric_columns = ['Price', 'Total_Area(SQFT)', 'Price_per_SQFT', 'Total_Rooms', 'BHK']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        print(f"Column {col} non-null count: {df[col].count()}")
    
    # Include Balcony in required columns
    required_columns = ['Property_Name', 'Location', 'city', 'property_type', 'Balcony'] + numeric_columns
    
    df_cleaned = df[required_columns]
    print(f"After selecting required columns: {len(df_cleaned)} rows")
    
    df_cleaned = df_cleaned.fillna({
        'Property_Name': 'Unknown',
        'Location': 'Unknown',
        'city': 'Unknown',
        'property_type': 'Unknown',
        'Balcony': 0  # Fill missing Balcony with 0
    })
    
    df_cleaned = df_cleaned.dropna(subset=numeric_columns, how='all')
    print(f"After cleaning: {len(df_cleaned)} rows")
    
    # Create dimension tables (excluding Balcony)
    dim_property_types = df_cleaned[['property_type']].drop_duplicates()
    dim_property_types['property_type_id'] = range(1, len(dim_property_types) + 1)
    print(f"Property types: {len(dim_property_types)}")
    
    dim_cities = df_cleaned[['city']].drop_duplicates()
    dim_cities['city_id'] = range(1, len(dim_cities) + 1)
    print(f"Cities: {len(dim_cities)}")
    
    dim_locations = df_cleaned[['Location', 'city']].drop_duplicates()
    dim_locations = dim_locations.merge(dim_cities, on='city', how='left')
    dim_locations['location_id'] = range(1, len(dim_locations) + 1)
    dim_locations = dim_locations[['location_id', 'Location', 'city_id']]
    print(f"Locations: {len(dim_locations)}")
    
    # Room dimension without Balcony
    dim_rooms = df_cleaned[['Total_Rooms', 'BHK']].drop_duplicates()
    dim_rooms['room_config_id'] = range(1, len(dim_rooms) + 1)
    print(f"Room configurations: {len(dim_rooms)}")
    
    # Update fact table merges and include Balcony
    fact_properties = df_cleaned.merge(
        dim_property_types, on='property_type', how='left'
    ).merge(
        dim_locations[['location_id', 'Location']], on='Location', how='left'
    ).merge(
        dim_rooms, on=['Total_Rooms', 'BHK'], how='left'
    )
    
    fact_columns = [
        'Property_Name', 'Price', 'Total_Area(SQFT)', 'Price_per_SQFT',
        'property_type_id', 'location_id', 'room_config_id', 'Location', 'Balcony'
    ]
    fact_properties = fact_properties[fact_columns]
    fact_properties['property_id'] = range(1, len(fact_properties) + 1)
    print(f"Fact table rows: {len(fact_properties)}")
    
    return {
        'original_data': df_cleaned,
        'dim_property_types': dim_property_types,
        'dim_cities': dim_cities,
        'dim_locations': dim_locations,
        'dim_rooms': dim_rooms,
        'fact_properties': fact_properties
    }

def test_normalization(tables):
    test_results = {}
    
    # Basic validation tests remain the same
    test_results['row_count_match'] = len(tables['original_data']) == len(tables['fact_properties'])
    test_results['no_missing_foreign_keys'] = not any([
        tables['fact_properties']['property_type_id'].isnull().any(),
        tables['fact_properties']['location_id'].isnull().any(),
        tables['fact_properties']['room_config_id'].isnull().any()
    ])
    test_results['no_duplicate_keys'] = all([
        len(tables['dim_property_types']['property_type_id']) == 
            len(tables['dim_property_types']['property_type_id'].unique()),
        len(tables['dim_cities']['city_id']) == 
            len(tables['dim_cities']['city_id'].unique()),
        len(tables['dim_locations']['location_id']) == 
            len(tables['dim_locations']['location_id'].unique()),
        len(tables['dim_rooms']['room_config_id']) == 
            len(tables['dim_rooms']['room_config_id'].unique())
    ])
    
    try:
        # Reconstruct data without Balcony
        reconstructed_data = tables['fact_properties'].merge(
            tables['dim_property_types'][['property_type_id', 'property_type']],
            on='property_type_id',
            how='left'
        ).merge(
            tables['dim_locations'][['location_id', 'Location']],
            on=['location_id', 'Location'],
            how='left'
        ).merge(
            tables['dim_rooms'][['room_config_id', 'Total_Rooms', 'BHK']],  # Removed Balcony
            on='room_config_id',
            how='left'
        )
        
        # Sample with consistent indices
        sample_size = min(10, len(tables['original_data']))
        sample_properties = np.random.choice(
            tables['original_data']['Property_Name'].unique(), 
            sample_size, 
            replace=False
        )
        
        original_samples = tables['original_data'][
            tables['original_data']['Property_Name'].isin(sample_properties)
        ].sort_values('Property_Name')
        
        reconstructed_samples = reconstructed_data[
            reconstructed_data['Property_Name'].isin(sample_properties)
        ].sort_values('Property_Name')
        
        # Compare attributes with aligned samples
        attributes_match = all([
            set(original_samples['Property_Name']) == set(reconstructed_samples['Property_Name']),
            set(original_samples['Location']) == set(reconstructed_samples['Location']),
            set(original_samples['property_type']) == set(reconstructed_samples['property_type']),
            np.array_equal(
                original_samples['Price'].fillna(0).values,
                reconstructed_samples['Price'].fillna(0).values
            )
        ])
        
        test_results['data_consistency'] = attributes_match
        
    except Exception as e:
        print(f"Error during reconstruction: {str(e)}")
        print("Original shape:", original_samples.shape if 'original_samples' in locals() else 'Not created')
        print("Reconstructed shape:", reconstructed_samples.shape if 'reconstructed_samples' in locals() else 'Not created')
        test_results['data_consistency'] = False
    
    return test_results
    
def save_normalized_tables(tables):
    """Save normalized tables to CSV files"""
    # Create directory if it doesn't exist
    Path("data/source 2/normalized").mkdir(parents=True, exist_ok=True)
    
    # Save dimension tables
    tables['dim_property_types'].to_csv('data/source 2/normalized/property_types.csv', index=False)
    tables['dim_cities'].to_csv('data/source 2/normalized/cities.csv', index=False)
    tables['dim_locations'].to_csv('data/source 2/normalized/locations.csv', index=False)
    tables['dim_rooms'].to_csv('data/source 2/normalized/rooms.csv', index=False)
    
    # Save fact table
    tables['fact_properties'].to_csv('data/source 2/normalized/properties.csv', index=False)
    
    print("\nTables saved successfully:")
    print("-" * 30)
    print("dim_property_types.csv")
    print("dim_cities.csv") 
    print("dim_locations.csv")
    print("dim_rooms.csv")
    print("fact_properties.csv")

def main():
    # Run normalization
    tables = normalize_real_estate_data('data/source 2/Indian_Real_Estate_Clean_Data.csv')
    
    # Run tests
    test_results = test_normalization(tables)
    save_normalized_tables(tables)

    
    # Display results
    print("\nNormalization Test Results:")
    print("-" * 30)
    for test_name, result in test_results.items():
        print(f"{test_name}: {'✓ Passed' if result else '✗ Failed'}")
    
    if not all(test_results.values()):
        print("\nWarning: Some tests failed. Please review the normalization process.")
    
    print("\nData Statistics:")
    print("-" * 30)
    for table_name, table in tables.items():
        print(f"{table_name}: {len(table)} rows")

if __name__ == "__main__":
    main()