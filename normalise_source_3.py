import pandas as pd
import os
from pathlib import Path

# Define the base directory
base_dir = Path('data/source 3')

# Ensure the directory exists
output_dir = base_dir / 'normalized'
output_dir.mkdir(parents=True, exist_ok=True)

# Read the CSV file
csv_file = base_dir / 'Real Estate Data V21.csv'
df = pd.read_csv(csv_file)

# Create Properties DataFrame
properties_df = df[['Name', 'Property Title', 'Description', 'Location', 'Total_Area']].copy()
properties_df['PropertyID'] = properties_df.index + 1

# Create Pricing DataFrame
pricing_df = df[['Price', 'Price_per_SQFT']].copy()
pricing_df['PropertyID'] = properties_df['PropertyID']
pricing_df['PriceID'] = pricing_df.index + 1

# Create Location DataFrame
location_df = df[['Location']].drop_duplicates().copy()
location_df['LocationID'] = location_df.index + 1

# Merge LocationID into Properties DataFrame
properties_df = properties_df.merge(location_df, on='Location')
properties_df = properties_df[['PropertyID', 'Name', 'Property Title', 'Description', 'LocationID', 'Total_Area']]

# Create Features DataFrame
features_df = df[['Baths', 'Balcony']].copy()
features_df['PropertyID'] = properties_df['PropertyID']
features_df['FeatureID'] = features_df.index + 1

# Rename columns
properties_df.columns = ['PropertyID', 'Name', 'Title', 'Description', 'LocationID', 'Total_Area']
pricing_df.columns = ['Price', 'Price_per_SQFT', 'PropertyID', 'PriceID']
location_df.columns = ['Location', 'LocationID']
features_df.columns = ['Baths', 'Balcony', 'PropertyID', 'FeatureID']

# Save to CSV files
properties_df.to_csv(output_dir / 'properties.csv', index=False)
pricing_df.to_csv(output_dir / 'pricing.csv', index=False)
location_df.to_csv(output_dir / 'location.csv', index=False)
features_df.to_csv(output_dir / 'features.csv', index=False)

# Reconstruct the original table for verification
reconstructed_df = properties_df.merge(location_df, on='LocationID')
reconstructed_df = reconstructed_df.merge(pricing_df[['Price', 'Price_per_SQFT', 'PropertyID']], on='PropertyID')
reconstructed_df = reconstructed_df.merge(features_df[['Baths', 'Balcony', 'PropertyID']], on='PropertyID')
reconstructed_df = reconstructed_df[['Name', 'Title', 'Price', 'Location', 'Total_Area', 'Price_per_SQFT', 'Description', 'Baths', 'Balcony']]

# Verify if the reconstruction matches the original DataFrame
reconstructed_df.columns = ['Name', 'Property Title', 'Price', 'Location', 'Total_Area', 'Price_per_SQFT', 'Description', 'Baths', 'Balcony']
if df.equals(reconstructed_df):
    print("The conversion was correct.")
else:
    print("The conversion was incorrect.")
    for column in df.columns:
        if not df[column].equals(reconstructed_df[column]):
            print(f"Mismatch found in column: {column}")
            print("Original DataFrame:")
            print(df[column].head())
            print("Reconstructed DataFrame:")
            print(reconstructed_df[column].head())