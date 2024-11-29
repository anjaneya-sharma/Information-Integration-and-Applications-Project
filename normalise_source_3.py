import pandas as pd
import os

# Ensure the directory exists
output_dir = 'data/source 3/normalized'
os.makedirs(output_dir, exist_ok=True)

# Read the CSV file
df = pd.read_csv(r'data/source 3/Real Estate Data V21.csv')

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
properties_df.to_csv('data/source 3/normalized/properties.csv', index=False)
pricing_df.to_csv('data/source 3/normalized/pricing.csv', index=False)
location_df.to_csv('data/source 3/normalized/location.csv', index=False)
features_df.to_csv('data/source 3/normalized/features.csv', index=False)

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