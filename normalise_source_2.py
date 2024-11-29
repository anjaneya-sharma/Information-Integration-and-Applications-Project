import pandas as pd
import numpy as np
from pathlib import Path

def create_directory_structure(base_dir):
    (base_dir / "normalized").mkdir(parents=True, exist_ok=True)

def normalize_real_estate_data(input_file_path):
    # Read data
    df = pd.read_csv(input_file_path)
    print(f"Initial data rows: {len(df)}")
    
    # Keep ALL original columns
    required_columns = [
        'Property_Name',
        'Property Title',
        'Price',
        'Location',
        'Total_Area(SQFT)',
        'Price_per_SQFT',
        'Description',
        'Total_Rooms',
        'Balcony',
        'city',
        'property_type',
        'BHK'
    ]
    
    df_cleaned = df[required_columns]
    print(f"After selecting required columns: {len(df_cleaned)} rows")
    
    # Create dimension tables
    dim_property_types = df_cleaned[['property_type']].drop_duplicates()
    dim_property_types['property_type_id'] = range(1, len(dim_property_types) + 1)
    
    dim_cities = df_cleaned[['city']].drop_duplicates()
    dim_cities['city_id'] = range(1, len(dim_cities) + 1)
    
    dim_locations = df_cleaned[['Location', 'city']].drop_duplicates()
    dim_locations = dim_locations.merge(dim_cities, on='city', how='left')
    dim_locations['location_id'] = range(1, len(dim_locations) + 1)
    dim_locations = dim_locations[['location_id', 'Location', 'city_id']]
    
    dim_rooms = df_cleaned[['Total_Rooms', 'BHK']].drop_duplicates()
    dim_rooms['room_config_id'] = range(1, len(dim_rooms) + 1)
    
    # Create fact table with ALL original attributes
    fact_properties = df_cleaned.merge(
        dim_property_types, on='property_type', how='left'
    ).merge(
        dim_locations[['location_id', 'Location']], on='Location', how='left'
    ).merge(
        dim_rooms, on=['Total_Rooms', 'BHK'], how='left'
    )
    
    # Include ALL original columns plus the generated IDs
    fact_columns = [
        'Property_Name',
        'Property Title',
        'Price',
        'Total_Area(SQFT)',
        'Price_per_SQFT',
        'property_type_id',
        'location_id',
        'room_config_id',
        'Location',
        'Balcony',
        'Description',
        'Total_Rooms',
        'BHK',
        'city',
        'property_type'
    ]
    
    fact_properties = fact_properties[fact_columns]
    fact_properties['property_id'] = range(1, len(fact_properties) + 1)
    
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
    
    # Basic validation tests
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
        # Use fact_properties directly since it already contains all needed columns
        reconstructed_data = tables['fact_properties']
        
        compare_columns = [
            'Property_Name',
            'Property Title',
            'Price',
            'Location',
            'Total_Area(SQFT)',
            'Price_per_SQFT',
            'Description',
            'Total_Rooms',
            'Balcony',
            'city',
            'property_type',
            'BHK'
        ]
        
        # Sample with consistent indices
        sample_size = min(10, len(tables['original_data']))
        sample_properties = np.random.choice(
            tables['original_data']['Property_Name'].unique(), 
            sample_size, 
            replace=False
        )
        
        original_samples = tables['original_data'][
            tables['original_data']['Property_Name'].isin(sample_properties)
        ].sort_values('Property_Name')[compare_columns]
        
        reconstructed_samples = reconstructed_data[
            reconstructed_data['Property_Name'].isin(sample_properties)
        ].sort_values('Property_Name')[compare_columns]
        
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
    
def save_normalized_tables(tables, base_dir):
    """Save normalized tables to CSV files"""
    # Create directory if it doesn't exist
    normalized_dir = base_dir / "normalized"
    normalized_dir.mkdir(parents=True, exist_ok=True)
    
    # Save dimension tables
    tables['dim_property_types'].to_csv(normalized_dir / 'property_types.csv', index=False)
    tables['dim_cities'].to_csv(normalized_dir / 'cities.csv', index=False)
    tables['dim_locations'].to_csv(normalized_dir / 'locations.csv', index=False)
    tables['dim_rooms'].to_csv(normalized_dir / 'rooms.csv', index=False)
    
    # Save fact table
    tables['fact_properties'].to_csv(normalized_dir / 'properties.csv', index=False)
    
    print("\nTables saved successfully:")
    print("-" * 30)
    print("property_types.csv")
    print("cities.csv") 
    print("locations.csv")
    print("rooms.csv")
    print("properties.csv")

def main():
    base_dir = Path("data/source 2")
    input_file_path = base_dir / "Indian_Real_Estate_Clean_Data.csv"
    
    # Run normalization
    tables = normalize_real_estate_data(input_file_path)
    
    # Run tests
    test_results = test_normalization(tables)
    save_normalized_tables(tables, base_dir)

    
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