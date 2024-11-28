import pandas as pd
from sqlalchemy import create_engine

# Database connection configuration
DATABASE_URL = 'postgresql://postgres:admin@localhost:5432/property_db'

# Create a connection to the PostgreSQL database
engine = create_engine(DATABASE_URL)

# Load normalized tables from PostgreSQL
property_details_df = pd.read_sql('property_details', engine)
property_features_df = pd.read_sql('property_features', engine)

# Note: Adjust the table names based on the actual names in your database

# Step 2: Merge the Normalized Tables Back
merged_df = pd.merge(property_details_df, property_features_df, on='property_id', how='inner')

# Step 3: Compare with the Original Source 3 Data
# Load the original Source 3 data from CSV for comparison
original_source3_df = pd.read_csv('data/source 3/Real Estate Data V21.csv')

# Rename columns in merged DataFrame to match the original Source 3 column names
merged_df.rename(columns={
    'property_name': 'Name',
    'property_title': 'Property Title',
    'total_area': 'Total_Area',
    # 'city_id': 'city',  # Assuming city names were replaced with city IDs, use appropriate mapping if necessary
    # 'property_type_id': 'property_type',  # Similar to city, adjust appropriately
}, inplace=True)

# Step 4: Check for Equality
# Compare merged DataFrame with the original to ensure normalization did not lose any data
comparison_result = merged_df.equals(original_source3_df)

if comparison_result:
    print("Normalization has been performed correctly. The merged data matches the original data.")
else:
    print("There are differences between the merged and original data. Please review the normalization process.")

# Optional: Display discrepancies
discrepancies = merged_df.compare(original_source3_df)
print("Discrepancies between merged and original data:\n", discrepancies)
